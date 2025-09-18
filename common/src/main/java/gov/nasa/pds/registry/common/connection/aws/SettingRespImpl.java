package gov.nasa.pds.registry.common.connection.aws;

import org.opensearch.client.opensearch.indices.GetIndicesSettingsResponse;
import org.opensearch.client.opensearch.indices.IndexState;
import gov.nasa.pds.registry.common.Response;

class SettingRespImpl implements Response.Settings {
  final private int replicas;
  final private int shards;
  SettingRespImpl (GetIndicesSettingsResponse response) {
    int r = -1, s = -1;
    for (IndexState state : response.result().values()) {
      // should only ever be one
      r = Integer.valueOf(state.settings().numberOfReplicas());
      s = Integer.valueOf(state.settings().numberOfShards());
    }
    this.replicas = r;
    this.shards = s;
  }
  @Override
  public int replicas() {
    return this.replicas;
  }

  @Override
  public int shards() {
    return this.shards;
  }
}
