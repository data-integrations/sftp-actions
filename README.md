SFTP Actions
============

[![Join CDAP community](https://cdap-users.herokuapp.com/badge.svg?t=sftp-actions)](https://cdap-users.herokuapp.com)
[![Build Status](https://travis-ci.org/hydrator/sftp-actions.svg?branch=develop)](https://travis-ci.org/hydrator/sftp-actions)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![CDAP Action](cdap-users.herokuapp.com/assets/cdap-action.svg)](http://docs.cask.co/cdap)

This repository contains CDAP Action plugins which can be used for performing SFTP operations.

* [SFTP Copy Action](docs/SFTPCopy-action.md)
* [SFTP Delete Action](docs/SFTPDelete-action.md)

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

```
    > load artifact <target/sftp-actions-<version>.jar config-file <target/sftp-actions-<version>.json>
```

For example, if your artifact is named 'sftp-actions-1.0.0':

```
    > load artifact target/sftp-actions-1.0.0.jar config-file target/sftp-actions-1.0.0.json
```

Mailing Lists
-------------

CDAP User Group and Development Discussions:

* `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from
users, release announcements, and any other discussions that we think will be helpful
to the users.

Slack Channel
-------------

CDAP Slack Channel: http://cdap-users.herokuapp.com

License and Trademarks
----------------------

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
