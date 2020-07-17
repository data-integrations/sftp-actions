# SCP Remote to Remote

Description
-----------
This Action will connect to a Bastion Host and execute a SCP command over SSH to copy a file path from Host A to Host B

Use Case
--------
The Use Cases for this Plugin is when we want to connect to a Bastion Host in order to execute an SCP command that will copy files between to other Remote hosts.

Properties
----------
| Configuration | Required | Default | Description |
| :------------ | :------: | :------ | :---------- |
| **Bastion Host** | **Y** | None | This is the Hostname for the Bastion host that will be executing the SCP command. Can be a hostname or IP Address |
| **Port** | **N** | 22 | Specifies the Port that will be used. Defaults to 22 |
| **Bastion User Name** | **Y** | None | Specifies the Bastion Hosts User Name. |
| **Private Key** | **Y** | None | The private RSA key to be used to connect over SHH to the Bastion host. This should be an RSA key. |
| **Passphrase** | **N** | None | Passphrase to be used with the Private RSA key if a Passphrase was setup when the key was created. |
| **Compression Flag** | **N** | None | Flag for SCP command to enable compression of files. |
| **Verbose Flag** | **N** | None | Flag for SCP command to enable verbose mode which extends Logs. |
| **Directory Flag** | **N** | None | Flag for SCP command to enable movement of directories. |
| **Host A User Name** | **Y** | None | The User Name of Host A. This is the host that the source files are on that need to be moved |
| **Host A Host** | **Y** | None |The Hostname of Host A. Can be a hostname or IP Address|
| **Source Path** | **Y** | None | The Absolute Path of the File that needs to be copied. |
| **Host B User Name** | **Y** | None | The User Name of Host B. This is the host that the source files are being copied to |
| **Host B Host** | **Y** | None |The Hostname of Host B. Can be a hostname or IP Address|
| **Destination Path** | **Y** | None | The Absolute Path of the location the files need to be copied to. |



Usage Notes
--------

In order to perform SCP between to remote hosts, we require a Bastion Host. An SCP command based on the configuration supplied will be created to perform a file copy. Authentication setup between all hosts will need to be setup before hand. This includes being able to SSH on the bastion host with the private key being supplied in the configuration and have the 2 remote hosts that files are being moved on having known host/authenticated_keys setup for SSH communication.