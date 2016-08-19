# dirac

A platform for downloading, converting and distributing data to any number of clients.

## usage

For Dirac to work, you must have access to some Openstack Swift object store, and a running Kafka server somewhere.

You must set your credentials for accessing the swift object store in the environment:

In some script on your machine:

```
#!/usr/bin/env bash

# store as .swift_account or whatever
unset -v OS_SERVICE_TOKEN OS_USERNAME OS_PASSWORD
unset -v OS_AUTH_URL OS_TENANT_NAME OS_REGION_NAME
# alter the following variables for your situation
export OS_USERNAME=${YOUR USERNAME}
export OS_PASSWORD=${YOUR PASSWORD}
export OS_TENANT_NAME=${PROJECT NAME HOSTING THE SWIFT OBJECT STORE}
export PS1='[\u SWIFT \W]\$ '
# DON'T alter these variables
export OS_AUTH_URL=${URL TO YOUR OPENSTACK INSTANCE}
export OS_REGION_NAME=${REGION OF YOUR OPENSTACK INSTANCE}

```

Run the script, then from python:

```
import dirac
dirac = dirac.DiracStore(swift_container, kafka_topic)
dirac.store(swift_path_root, path_to_file_on_local_system)
```

This will store the file in your object store under the given base and path, then send a resource message describing the file to the Kafka topic you have specified.

!