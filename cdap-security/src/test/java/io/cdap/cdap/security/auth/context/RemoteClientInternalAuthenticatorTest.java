/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.security.auth.context;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.HttpURLConnection;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Tests for verifying internal authentication for the {@link io.cdap.cdap.common.internal.remote.RemoteClient}.
 */
public class RemoteClientInternalAuthenticatorTest {
  private static final String TEST_SERVICE = "test";

  private static TestHttpHandler testHttpHandler;
  private static NettyHttpService httpService;
  private static Injector injector;

  @BeforeClass
  public static void setup() throws Exception {
    // Setup Guice injector.
    injector = Guice.createInjector(new ConfigModule(), new InMemoryDiscoveryModule(),
                                    new AuthenticationContextModules().getNoOpModule());
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    DiscoveryService discoveryService = injector.getInstance(DiscoveryService.class);

    // Setup test HTTP handler and register the service.
    testHttpHandler = new TestHttpHandler();
    httpService = new CommonNettyHttpServiceBuilder(cConf, TEST_SERVICE)
      .setHttpHandlers(testHttpHandler).build();
    httpService.start();
    discoveryService.register(new Discoverable(TEST_SERVICE, httpService.getBindAddress()));
  }

  @AfterClass
  public static void teardown() throws Exception {
    httpService.stop();
  }

  @Test
  public void testRemoteClientWithoutInternalAuthInjectsNoAuthenticationContext() throws Exception {
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    cConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, false);
    RemoteClientFactory remoteClientFactory = injector.getInstance(RemoteClientFactory.class);
    RemoteClient remoteClient = remoteClientFactory
      .createRemoteClient(TEST_SERVICE, new HttpRequestConfig(15000, 15000, false), "/");
    HttpURLConnection conn = remoteClient.openConnection(HttpMethod.GET, "");
    int responseCode = conn.getResponseCode();

    // Verify that the request received the expected headers.
    HttpHeaders headers = testHttpHandler.getRequest().headers();

    Assert.assertEquals(HttpResponseStatus.OK.code(), responseCode);
    Assert.assertFalse(headers.contains(Constants.Security.Headers.USER_ID));
    Assert.assertFalse(headers.contains(Constants.Security.Headers.RUNTIME_TOKEN));
  }

  @Test
  public void testRemoteClientWithInternalAuthInjectsAuthenticationContext() throws Exception {
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    cConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, true);
    RemoteClientFactory remoteClientFactory = injector.getInstance(RemoteClientFactory.class);
    RemoteClient remoteClient = remoteClientFactory
      .createRemoteClient(TEST_SERVICE, new HttpRequestConfig(15000, 15000, false), "/");

    // Set authentication context principal.
    String expectedName = "somebody";
    String expectedCredValue = "credential";
    Credential.CredentialType expectedCredType = Credential.CredentialType.EXTERNAL;
    System.setProperty("user.name", expectedName);
    System.setProperty("user.credential.value", expectedCredValue);
    System.setProperty("user.credential.type", expectedCredType.name());

    HttpURLConnection conn = remoteClient.openConnection(HttpMethod.GET, "");
    int responseCode = conn.getResponseCode();

    // Verify that the request received the expected headers.
    HttpHeaders headers = testHttpHandler.getRequest().headers();

    Assert.assertEquals(HttpResponseStatus.OK.code(), responseCode);
    Assert.assertEquals(expectedName, headers.get(Constants.Security.Headers.USER_ID));
    Assert.assertEquals(String.format("%s %s", expectedCredType.getQualifiedName(), expectedCredValue),
                        headers.get(Constants.Security.Headers.RUNTIME_TOKEN));
  }

  /**
   * HTTP handler for testing.
   */
  public static class TestHttpHandler extends AbstractHttpHandler {
    private HttpRequest request;

    @GET
    @Path("/")
    public void get(HttpRequest request, HttpResponder responder) {
      this.request = request;
      responder.sendStatus(HttpResponseStatus.OK);
    }

    public HttpRequest getRequest() {
      return request;
    }
  }
}
