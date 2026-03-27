package com.wayblink.iceberg.render;

import com.wayblink.iceberg.analyzer.AnalysisResult;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Value;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

public final class JsonRenderer {

  private final ObjectMapper objectMapper;
  private final JsonPayloadBuilder payloadBuilder;

  public JsonRenderer() {
    this.objectMapper = new ObjectMapper();
    this.payloadBuilder = new JsonPayloadBuilder();
    this.objectMapper.setSerializationInclusion(JsonInclude.Include.ALWAYS);
    this.objectMapper.configOverride(Map.class)
        .setInclude(Value.construct(JsonInclude.Include.ALWAYS, JsonInclude.Include.ALWAYS));
  }

  public String render(AnalysisResult result) {
    return render(payloadBuilder.analysis(result));
  }

  public String render(Object value) {
    try {
      return objectMapper.writeValueAsString(value);
    } catch (Exception exception) {
      throw new IllegalStateException("Failed to render value as JSON", exception);
    }
  }
}
