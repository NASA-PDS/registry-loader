package gov.nasa.pds.registry.common.connection.es;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import gov.nasa.pds.registry.common.Request;
import gov.nasa.pds.registry.common.connection.Direct;

class BulkWrapper implements Request.Bulk {
  final HttpURLConnection con;
  final OutputStreamWriter writer;
  BulkWrapper(Direct conFactory) throws IOException {
    this.con = conFactory.createConnection();
    this.con.setDoInput(true);
    this.con.setDoOutput(true);
    this.con.setRequestMethod("POST");
    this.con.setRequestProperty("content-type", "application/x-ndjson; charset=utf-8");
    this.writer = new OutputStreamWriter(con.getOutputStream(), "UTF-8");
  }
  @Override
  public void add(String statement) throws IOException {
    this.writer.write(statement);
    this.writer.write("\n");
  }
}
