/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.common;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginConfig;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Common configurations for the FTP Action plugins.
 */
public class SFTPActionConfig extends PluginConfig {
  @Description("Host name of the SFTP server.")
  @Macro
  public String host;

  @Description("Port on which SFTP server is running. Defaults to 22.")
  @Nullable
  @Macro
  public Integer port;

  @Description("Name of the user used to login to SFTP server.")
  @Macro
  public String userName;

  @Description("Private Key to be used to login to SFTP Server. SSH key must be of RSA type")
  @Macro
  @Nullable
  public String privateKey;

  @Description("Passphrase to be used with private key if passphrase was enabled when key was created. " +
          "If PrivateKey is selected for Authentication")
  @Macro
  @Nullable
  public String passphrase;

  @Name("Authentication")
  @Description("Authentication type to be used for connection")
  public String authTypeBeingUsed;

  @Description("Password used to login to SFTP server. If Password is selected for Authentication")
  @Macro
  @Nullable
  public String password;

  @Description("Properties that will be used to configure the SSH connection to the FTP server. " +
    "For example to enable verbose logging add property 'LogLevel' with value 'VERBOSE'. " +
    "To enable host key checking set 'StrictHostKeyChecking' to 'yes'. " +
    "SSH can be configured with the properties described here 'https://linux.die.net/man/5/ssh_config'.")
  @Nullable
  public String sshProperties;

  public String getHost() {
    return host;
  }

  public int getPort() { return (port != null) ? port : 22; }

  public String getUserName() {
    return userName;
  }

  public String getPassword() {
    return password;
  }

  public byte[] getPrivateKey() { return privateKey.getBytes(StandardCharsets.UTF_8); }

  public String getAuthTypeBeingUsed() { return authTypeBeingUsed; }

  public byte[] getPassphrase(){
    return passphrase == null ? new byte[0] : passphrase.getBytes(StandardCharsets.UTF_8); }

  public Map<String, String> getSSHProperties(){
    Map<String, String> properties = new HashMap<>();
    // Default set to no
    properties.put("StrictHostKeyChecking", "no");
    if (sshProperties == null || sshProperties.isEmpty()) {
      return properties;
    }
    KeyValueListParser kvParser = new KeyValueListParser("\\s*,\\s*", ":");
    for (KeyValue<String, String> keyVal : kvParser.parse(sshProperties)) {
      String key = keyVal.getKey();
      String val = keyVal.getValue();
      properties.put(key, val);
    }
    return properties;
  }
}
