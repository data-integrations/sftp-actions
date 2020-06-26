SFTP Delete
===========

<a href="https://cdap-users.herokuapp.com/"><img alt="Join CDAP community" src="https://cdap-users.herokuapp.com/badge.svg?t=sftp-actions"/></a>
[![Build Status](https://travis-ci.org/hydrator/sftp-actions.svg?branch=develop)](https://travis-ci.org/hydrator/sftp-actions) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) <img src="https://cdap-users.herokuapp.com/assets/cdap-action.svg"/>


CDAP Action plugin to delete the specified files from the SFTP server.


Use Case
--------
This plugin is generally used along with the SFTP copy plugin. SFTP copy plugin copies the files from SFTP
server to the desired location such as `HDFS`. Once copied to `HDFS`, files can be processed by pipeline.
When the pipeline is successful, the files from SFTP server can be deleted. SFTP delete plugin can be used in
the end of the pipeline to achieve this.

Plugin Configuration
--------------------

| Configuration | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Host** | **Y** | N/A | Specifies the host name of the SFTP server.|
| **Port** | **N** | 22 | Specifies the port on which SFTP server is running.|
| **User Name** | **Y** | N/A | Specifies the name of the user which will be used to connect to the SFTP server.|
|**Authentication**|**Y**|**PrivateKey**| Specifies the type of Authentication that will be used to connect to the SFTP Server|
|**Private Key**|**N**|N/A| Private RSA Key to be used to connect to the SFTP Server. This key can be stored in the Secure Keys Store and macro called into the Configuration. Must be a RSA key example: -----BEGIN RSA PRIVATE KEY-----|
|**Private Key Passphrase**|**N**|N/A| Passphrase to be used with RSA Private Key if a Passphrase was specified when key was generated|
| **Password** | **N** | N/A | Specifies the password of the user. Only Required if Private Key is not being used|
| **Files to be deleted** | **Y** | ${sftp.copied.file.names} | Comma separated list of files on the SFTP server to be deleted. Default value for this field is a Macro which will be substituted by SFTP copy plugin when this plugin is used with it. |
| **Destination Directory** | **Y** | N/A | Destination directory on the file system, where files need to be copied. If directory does not exist, it will lbe created.|
| **Continue execution on error** | **N** | false | Boolean flag to determine whether to proceed with next files in case there is a failure in deletion of any particular file. |
| **Properties for SSH** | **N** | N/A | Specifies the properties that are used to configure SSH connection to the FTP server. For example to enable verbose logging add property 'LogLevel' with value 'VERBOSE'. To enable host key checking set 'StrictHostKeyChecking' to 'yes'. SSH can be configured with the properties described here 'https://linux.die.net/man/5/ssh_config'. |


Build
-----
To build this plugin:

```
   mvn clean package
```

The build will create a .jar and .json file under the ``target`` directory.
These files can be used to deploy your plugins.

Deployment
----------
You can deploy your plugins using the CDAP CLI:

    > load artifact <target/sftp-actions-<version>.jar config-file <target/sftp-actions-<version>.json>

For example, if your artifact is named 'sftp-actions-1.0.0':

    > load artifact target/sftp-actions-1.0.0.jar config-file target/sftp-actions-1.0.0.json

## Mailing Lists

CDAP User Group and Development Discussions:

* `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from
users, release announcements, and any other discussions that we think will be helpful
to the users.

## Slack Channel

CDAP Slack Channel: http://cdap-users.herokuapp.com/


## License and Trademarks

Copyright © 2017 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.