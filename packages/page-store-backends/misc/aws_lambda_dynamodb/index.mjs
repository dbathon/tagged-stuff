import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, GetCommand, PutCommand, DeleteCommand, QueryCommand } from "@aws-sdk/lib-dynamodb";

const TABLE_NAME = "KeyValueStore";

const client = new DynamoDBClient({});
const dynamoDB = DynamoDBDocumentClient.from(client);

export async function handler(event) {
  const { rawPath, body: bodyString } = event;
  const httpMethod = event.requestContext?.http?.method;
  if (httpMethod !== "POST") {
    return { statusCode: 400, body: { error: "Invalid request" } };
  }

  const pathParts = rawPath.match(/^\/(\w+)$/);
  if (!pathParts) {
    return { statusCode: 400, body: { error: "Invalid path" } };
  }
  const scope = pathParts[1];

  try {
    const body = JSON.parse(bodyString);
    if (!body || typeof body !== "object") {
      return { statusCode: 400, body: { error: "Invalid request body" } };
    }
    const operationName = body.operation;
    if (!operationNames.includes(operationName)) {
      return { statusCode: 400, body: { error: "Invalid operation" } };
    }
    const operation = operations[operationName];
    for (const requiredParam of operation.required) {
      if (typeof body[requiredParam] !== "string") {
        return { statusCode: 400, body: { error: `Missing required parameter: ${requiredParam}` } };
      }
    }
    return await operation.fn(scope, body);
  } catch (error) {
    return { statusCode: 500, body: { error: error.message } };
  }
}

const operations = {
  read: { fn: getValue, required: ["key"] },
  create: { fn: createValue, required: ["key", "version", "data"] },
  update: { fn: updateValue, required: ["key", "expectedVersion", "version", "data"] },
  delete: { fn: deleteValue, required: ["key", "expectedVersion"] },
  listKeys: { fn: listKeys, required: ["from", "to"] },
};
const operationNames = Object.keys(operations);

async function getValue(scope, body) {
  const params = new GetCommand({
    TableName: TABLE_NAME,
    Key: {
      EntryScope: scope,
      EntryKey: body.key,
    },
    ConsistentRead: true,
  });

  const result = await dynamoDB.send(params);
  return result.Item
    ? { statusCode: 200, body: { version: result.Item.EntryVersion, data: result.Item.EntryData } }
    : { statusCode: 404 };
}

async function createValue(scope, body) {
  const { key, version, data } = body;

  const params = new PutCommand({
    TableName: TABLE_NAME,
    Item: { EntryScope: scope, EntryKey: key, EntryVersion: version, EntryData: data },
    ConditionExpression: "attribute_not_exists(EntryScope)",
  });

  try {
    await dynamoDB.send(params);
    return { statusCode: 204 };
  } catch (error) {
    return { statusCode: 409, body: { error: "Version mismatch or key exists" } };
  }
}

async function updateValue(scope, body) {
  const { key, expectedVersion, version, data } = body;

  const params = new PutCommand({
    TableName: TABLE_NAME,
    Item: { EntryScope: scope, EntryKey: key, EntryVersion: version, EntryData: data },
    ConditionExpression: "EntryVersion = :expectedVersion",
    ExpressionAttributeValues: { ":expectedVersion": expectedVersion },
  });

  try {
    await dynamoDB.send(params);
    return { statusCode: 204 };
  } catch (error) {
    return { statusCode: 409, body: { error: "Version mismatch or key exists" } };
  }
}

async function deleteValue(scope, body) {
  const { key, expectedVersion } = body;

  const params = new DeleteCommand({
    TableName: TABLE_NAME,
    Key: {
      EntryScope: scope,
      EntryKey: key,
    },
    ConditionExpression: "EntryVersion = :expectedVersion",
    ExpressionAttributeValues: { ":expectedVersion": expectedVersion },
  });

  try {
    await dynamoDB.send(params);
    return { statusCode: 204 };
  } catch (error) {
    return { statusCode: 404, body: { error: "Key not found or version mismatch" } };
  }
}

async function listKeys(scope, body) {
  const { from, to } = body;

  const command = new QueryCommand({
    TableName: TABLE_NAME,
    KeyConditionExpression: "EntryScope = :scope AND EntryKey BETWEEN :from AND :to",
    ExpressionAttributeValues: {
      ":scope": scope,
      ":from": from,
      ":to": to,
    },
    ConsistentRead: true,
  });

  const result = await dynamoDB.send(command);
  const keys = result.Items.map((item) => item.EntryKey);
  return { statusCode: 200, body: { keys } };
}
