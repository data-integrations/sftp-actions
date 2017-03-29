SFTP Copy
=========

<a href="https://cdap-users.herokuapp.com/"><img alt="Join CDAP community" src="https://cdap-users.herokuapp.com/badge.svg?t=sftp-actions"/></a>
[![Build Status](https://travis-ci.org/hydrator/sftp-actions.svg?branch=develop)](https://travis-ci.org/hydrator/sftp-actions) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) <img src="https://cdap-users.herokuapp.com/assets/cdap-action.svg"/>


CDAP Action plugin to copy files from the SFTP server to the specified destination.


Usage Notes
-----------
In a typical enterprise setting, it is common to have SFTP server which allows sharing of the files over network.
Various individuals and applications can upload files to the SFTP server. Consumers of these files can download
them from SFTP server for further processing. Once processed, the files on the SFTP server are no longer required
and can be deleted.

SFTP Copy plugin targets the use case where the files can be copied from SFTP server and stored on the desired
destination such as `HDFS`. If files on the SFTP server are in compressed .zip format, then plugin can optionally
un-compress them while copying.


Plugin Configuration
--------------------

| Configuration | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Host** | **Y** | N/A | Specifies the host name of the SFTP server.|
| **Port** | **N** | 22 | Specifies the port on which SFTP server is running.|
| **User** | **Y** | N/A | Specifies the name of the user which will be used to connect to the SFTP server.|
| **Password** | **Y** | N/A | Specifies the password of the user.|
| **Source Directory** | **Y** | N/A | Absolute path of the directory on the SFTP server which is to be copied. If the directory is empty, the execution of the plugin will be no-op.|
| **Destination Directory** | **Y** | N/A | Destination directory on the file system, where files need to be copied. If directory does not exist, it will lbe created.|
| **Extract zip files** | **N** | true | Boolean flag to determine whether to extract .zip files while copying.|
| **Variable name to hold list of copied file names** | **N** | sftp.copied.file.names | Name of the variable which holds comma separated list of file names on the SFTP server which were copied during this run of the plugin. Usually this variable is used as Macro in the SFTP Delete action to delete the files from SFTP server once their processing is successful. |
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