/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.hubspot.common;

import com.google.common.io.CharStreams;
import com.google.gson.Gson;
import io.cdap.cdap.api.ServiceDiscoverer;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;

/**
 * Dirty hacks to get access to things not exposed through cdap-etl-api. Very tightly coupled to how CDAP has
 * implemented Spark pipelines.
 */
public class Hacks {

  private Hacks() {
    // no-op constructor for util class
  }

  /**
   * Get an OAuthTokenRefresher. This will only work if it is called from within the Hadoop InputFormat.
   * For example, if it is called in HubspotInputFormat.createRecordReader.
   *
   * @return an OAuthTokenRefresher
   * @throws Exception if any number of things goes wrong due to this being a hack
   */
  public static OAuthTokenRefresher getTokenRefresher(String provider, String credentialId) throws Exception {
    ServiceDiscoverer serviceDiscoverer = getServiceDiscovererFromClassloader();
    return new OAuthTokenRefresher(serviceDiscoverer, provider, credentialId);
  }

  private static ServiceDiscoverer getServiceDiscovererFromClassloader()
    throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, ClassNotFoundException {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Class<?> sparkRuntimeContextProviderClass =
      findClass(cl, "io.cdap.cdap.app.runtime.spark.SparkRuntimeContextProvider");

    Method getMethod = sparkRuntimeContextProviderClass.getMethod("get");
    return (ServiceDiscoverer) getMethod.invoke(null);
  }

  private static Class<?> findClass(ClassLoader cl, String className) throws ClassNotFoundException {
    while (cl != null) {
      try {
        Class<?> clz = cl.loadClass(className);
        if (clz != null) {
          return clz;
        }
      } catch (ClassNotFoundException e) {
        // move on
      }
      cl = cl.getParent();
    }
    throw new ClassNotFoundException();
  }

  /**
   * Makes a service call back to CDAP to refresh an OAuth token.
   */
  public static class OAuthTokenRefresher {
    private static final Gson GSON = new Gson();
    private final ServiceDiscoverer serviceDiscoverer;
    private final String provider;
    private final String credentialId;

    OAuthTokenRefresher(ServiceDiscoverer serviceDiscoverer, String provider, String credentialId) {
      this.serviceDiscoverer = serviceDiscoverer;
      this.provider = provider;
      this.credentialId = credentialId;
    }

    /**
     * Send a request back to CDAP to get an OAuth access token. This is the same thing that happens during
     * OAuth macro resolution.
     */
    public OAuthInfo refresh() throws IOException {
      String resource = String.format("v1/oauth/provider/%s/credential/%s", provider, credentialId);
      URLConnection conn = serviceDiscoverer.openConnection("system", "pipeline", "studio", resource);
      HttpURLConnection httpConn = (HttpURLConnection) conn;
      try (Reader reader = new InputStreamReader(httpConn.getInputStream(), StandardCharsets.UTF_8)) {
        return GSON.fromJson(reader, OAuthInfo.class);
      } finally {
        httpConn.disconnect();
      }
    }

    public String getAuthURL() throws IOException {
      String resource = String.format("v1/oauth/provider/%s/authurl", provider);
      URLConnection conn = serviceDiscoverer.openConnection("system", "pipeline", "studio", resource);
      HttpURLConnection httpConn = (HttpURLConnection) conn;
      try (Reader reader = new InputStreamReader(httpConn.getInputStream(), StandardCharsets.UTF_8)) {
        return CharStreams.toString(reader);
      } finally {
        httpConn.disconnect();
      }
    }
  }
}
