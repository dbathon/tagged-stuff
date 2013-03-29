package dbathon.web.taggedstuff.gson.adaptor;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;
import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

public class DateAsTimestampTypeAdaptor extends TypeAdapter<Date> {

  public static final TypeAdapterFactory FACTORY = new TypeAdapterFactory() {
    @Override
    @SuppressWarnings("unchecked")
    public <T> TypeAdapter<T> create(final Gson gson, final TypeToken<T> typeToken) {
      final Class<? super T> rawType = typeToken.getRawType();
      final boolean matchingType = rawType == Date.class || rawType == Timestamp.class;
      return matchingType ? (TypeAdapter<T>) new DateAsTimestampTypeAdaptor(rawType) : null;
    }
  };

  private final Class<?> type;

  public DateAsTimestampTypeAdaptor(final Class<?> type) {
    this.type = type;
  }

  @Override
  public Date read(final JsonReader in) throws IOException {
    if (in.peek() == JsonToken.NULL) {
      in.nextNull();
      return null;
    }
    return toType(in.nextLong());
  }

  private Date toType(final long timestamp) {
    if (type == Date.class) {
      return new Date(timestamp);
    }
    else if (type == Timestamp.class) {
      return new Timestamp(timestamp);
    }
    else {
      throw new IllegalArgumentException("unsupported type: " + type);
    }
  }

  @Override
  public void write(final JsonWriter out, final Date value) throws IOException {
    // just write the long timestamp as string
    if (value == null) {
      out.nullValue();
      return;
    }
    out.value(Long.toString(value.getTime()));
  }

}
