SFTP Copy
=========

<a href="https://cdap-users.herokuapp.com/"><img alt="Join CDAP community" src="https://cdap-users.herokuapp.com/badge.svg?t=sftp-actions"/></a>
[![Build Status](https://travis-ci.org/hydrator/sftp-actions.svg?branch=develop)](https://travis-ci.org/hydrator/sftp-actions) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) <img src="https://cdap-users.herokuapp.com/assets/cdap-action.svg"/>


SFTP copy allows copying of the files from the specified directory on SFTP servers and write them to HDFS as the destination.
The files that are copied can be optionally uncompressed before storing. The files are copied directly to HDFS without needing any additional staging area.


Usage Notes
-----------
In order perform SFTP copy, we require host and port on which the SFTP server is running. SFTP implements secure file
transfer over SSH. Typically port number 22 is used for SFTP(which is also default port for SSH). We also require valid
credentials in the form of user name and password. Please make sure that you are able to SSH to the SFTP server using
specified user and password. SSH connection to SFTP server might require some additional configurations such as to enable
host key checking set 'StrictHostKeyChecking' to 'yes'. These additional configurations can be specified using
`Properties for SSH` section.

Directory on the SFTP server which needs to be copied can be specified using `Source directory` property. The specified
directory should exist and absolute path to the directory must be provided. If directory is empty then execution will
continue without any error. `Destination directory` is the absolute path of the directory on HDFS where files need to be copied.
If destination directory does not exists, then it will be created first. If file with the same name already exists in
the destination directory, it will be overwritten.

Files from the SFTP server can be optionally be uncompressed while copying to HDFS. Currently uncompress option is only supported
for the zip files.

Files on the SFTP server may required to be deleted, once they are processed. The comma separated list of file names on the
SFTP server which were copied to HDFS in the current run are stored in a variable named `sftp.copied.file.names`.
SFTP Delete action can be configured to run at the end of the pipeline, which uses this variable to delete the files on the SFTP server.

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
| **Uncompress** | **N** | true | Boolean flag to determine whether to uncompress the `.zip` files while copying.|
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