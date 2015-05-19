![alt tag](https://travis-ci.org/uralian/ignition.svg?branch=master)

# Ignition
Creating reusable workflows for Apache Spark

## Scripting

Ignition supports the following syntax for injecting data:

* %{var_name} - injects a previously set variable
* ${field_name} - injects the value of the specified field (per each row)
* #{env_name} - injects the value of the JVM environment property
