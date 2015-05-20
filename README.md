# ignition
Creating reusable workflows for Apache Spark

![alt tag](https://travis-ci.org/uralian/ignition.svg?branch=master)
[![Coverage Status](https://coveralls.io/repos/uralian/ignition/badge.svg)](https://coveralls.io/r/uralian/ignition)

## Scripting

Ignition supports the following syntax for injecting data:

* %{var_name} - injects a previously set variable
* ${field_name} - injects the value of the specified field (per each row)
* #{env_name} - injects the value of the JVM environment property

In cases involving string interpolation, the following prefixes are recognized:

* v"var_name" - a previously set variable
* $"field_name" - the value of the specified field (per each row)
* e"env_name" - the value of the JVM environment property
