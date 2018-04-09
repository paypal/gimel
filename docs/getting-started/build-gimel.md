

## Pre-requisite

Maven is required to build gimel.

You may find more information [here](https://maven.apache.org/index.html).

## Maven Build

Clone git repository
```bash
git clone git@github.com:paypal/gimel.git
OR
git clone https://github.com/paypal/gimel.git
cd gimel
```

## Maven Profiles

Run below command to build
(-T 8 is to run 8 tasks in parallel; reduces the build time considerably)

| Profile | Command | Notes |
| -------- | -------- | -------- |
| Default | ```build/gimel install -T 8 -B``` | Recommended. Builds with all dependencies pulled from maven central - profile general |
| General | ```build/gimel install -T 8 -B -Pgeneral``` | Builds with all dependencies pulled from maven central |
| HWX releases | ```build/gimel clean install -T 8 -B -Phwx-2.6.3.11-1``` | Builds with all dependencies pulled from horton works repo if available |
| Stand Alone | ```build/gimel clean install -T 8 -B -Pstandalone``` | Builds gimel with scala packages bundled in jar, used for standalone execution of gimel jar / polling services |

--------------------------------------------------------------------------------------------------------------------


# Gimel Modules (UML)

Below is the dependency graph of Gimel Modules.

<img src="../../images/gimel-modules.png" width="800" height="600" />

--------------------------------------------------------------------------------------------------------------------
