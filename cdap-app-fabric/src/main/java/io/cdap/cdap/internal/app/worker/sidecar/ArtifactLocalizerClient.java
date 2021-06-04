/*
 * Copyright © 2021 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.internal.app.worker.sidecar;

import com.google.inject.Inject;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

public class ArtifactLocalizerClient {

  private final String sidecarBaseURL;
  private LocationFactory locationFactory;

  @Inject
  ArtifactLocalizerClient(CConfiguration cConf, LocationFactory locationFactory) {
    this.sidecarBaseURL = String
      .format("http://127.0.0.1:%d/%s/worker", cConf.getInt(Constants.ArtifactLocalizer.PORT),
              Constants.Gateway.INTERNAL_API_VERSION_3_TOKEN);
    this.locationFactory = locationFactory;
  }

  public Location getArtifactLocation(ArtifactId artifactId) throws IOException, ArtifactNotFoundException {
    return sendRequest(artifactId, false);
  }

  public Location getUnpackedArtifactLocation(ArtifactId artifactId) throws IOException, ArtifactNotFoundException {
    return sendRequest(artifactId, true);
  }

  private Location sendRequest(ArtifactId artifactId, boolean unpack) throws IOException, ArtifactNotFoundException {
    String urlPath = String
      .format("/artifact/namespaces/%s/artifacts/%s/versions/%s?unpack=%b", artifactId.getNamespace(),
              artifactId.getArtifact(),
              artifactId.getVersion(), unpack);
    URL url = null;
    try {
      url = new URI(sidecarBaseURL + urlPath).toURL();
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }

    HttpRequest httpRequest = HttpRequest.builder(HttpMethod.GET, url).build();
    HttpResponse httpResponse = HttpRequests.execute(httpRequest);
    if (httpResponse.getResponseCode() != HttpURLConnection.HTTP_OK) {
      if (httpResponse.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
        throw new ArtifactNotFoundException(artifactId);
      }
      throw new IOException(httpResponse.getResponseBodyAsString());
    }

    String path = httpResponse.getResponseBodyAsString();
    Location location = Locations.getLocationFromAbsolutePath(locationFactory, path);
    return location;
  }
}
