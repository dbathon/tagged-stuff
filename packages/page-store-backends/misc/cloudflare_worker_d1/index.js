/*
Setup: create a D1 database with the following table:

CREATE TABLE key_value_store (
  entry_scope   TEXT NOT NULL,
  entry_key     TEXT NOT NULL,
  entry_version TEXT NOT NULL,
  entry_data    TEXT NOT NULL,
  PRIMARY KEY (entry_scope, entry_key)
);

And bind it with the name DB to the worker.
*/

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
};

function cors(response) {
  const headers = new Headers(response.headers);
  for (const [k, v] of Object.entries(CORS_HEADERS)) {
    headers.set(k, v);
  }
  return new Response(response.body, {
    status: response.status,
    headers,
  });
}

function json(status, body) {
  return cors(
    new Response(JSON.stringify(body), {
      status,
      headers: { "Content-Type": "application/json" },
    }),
  );
}

function error(status) {
  return cors(new Response(null, { status }));
}

export default {
  async fetch(request, env) {
    // CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: CORS_HEADERS,
      });
    }

    if (request.method !== "POST") {
      return json(400, { error: "Invalid request" });
    }

    const url = new URL(request.url);
    const match = url.pathname.match(/^\/(\w+)$/);
    if (!match) {
      return json(400, { error: "Invalid path" });
    }

    const scope = match[1];

    let body;
    try {
      body = await request.json();
    } catch {
      return json(400, { error: "Invalid request body" });
    }

    if (!body || typeof body !== "object") {
      return json(400, { error: "Invalid request body" });
    }

    const operation = operations[body.operation];
    if (!operation) {
      return json(400, { error: "Invalid operation" });
    }

    for (const param of operation.required) {
      if (typeof body[param] !== "string") {
        return json(400, { error: `Missing required parameter: ${param}` });
      }
    }

    try {
      return await operation.fn(env.DB, scope, body);
    } catch (err) {
      return json(500, { error: err.message });
    }
  },
};

const operations = {
  read: { fn: getValue, required: ["key"] },
  create: { fn: createValue, required: ["key", "version", "data"] },
  update: { fn: updateValue, required: ["key", "expectedVersion", "version", "data"] },
  delete: { fn: deleteValue, required: ["key", "expectedVersion"] },
  listKeys: { fn: listKeys, required: ["from", "to"] },
};

async function getValue(db, scope, body) {
  const row = await db
    .prepare("SELECT entry_version, entry_data FROM key_value_store WHERE entry_scope = ? AND entry_key = ?")
    .bind(scope, body.key)
    .first();

  if (!row) {
    return error(404);
  }

  return json(200, {
    version: row.entry_version,
    data: row.entry_data,
  });
}

async function createValue(db, scope, body) {
  const { key, version, data } = body;

  const result = await db
    .prepare("INSERT INTO key_value_store (entry_scope, entry_key, entry_version, entry_data) VALUES (?, ?, ?, ?)")
    .bind(scope, key, version, data)
    .run();

  if (!result.success || result.meta.rows_written < 1) {
    return json(409, { error: "Version mismatch or key exists" });
  }

  return error(204);
}

async function updateValue(db, scope, body) {
  const { key, expectedVersion, version, data } = body;

  const result = await db
    .prepare(
      "UPDATE key_value_store SET entry_version = ?, entry_data = ? " +
        "WHERE entry_scope = ? AND entry_key = ? AND entry_version = ?",
    )
    .bind(version, data, scope, key, expectedVersion)
    .run();

  if (!result.success || result.meta.rows_written < 1) {
    return json(409, { error: "Version mismatch or key exists" });
  }

  return error(204);
}

async function deleteValue(db, scope, body) {
  const { key, expectedVersion } = body;

  const result = await db
    .prepare("DELETE FROM key_value_store WHERE entry_scope = ? AND entry_key = ? AND entry_version = ?")
    .bind(scope, key, expectedVersion)
    .run();

  if (!result.success || result.meta.rows_written < 1) {
    return json(404, { error: "Key not found or version mismatch" });
  }

  return error(204);
}

async function listKeys(db, scope, body) {
  const { from, to } = body;

  const rows = await db
    .prepare(
      "SELECT entry_key FROM key_value_store WHERE entry_scope = ? AND entry_key BETWEEN ? AND ? ORDER BY entry_key",
    )
    .bind(scope, from, to)
    .all();

  return json(200, {
    keys: rows.results.map((r) => r.entry_key),
  });
}
