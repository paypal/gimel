
# Copyright 2019 PayPal Inc.
# 
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

Eclipse IDE Setup
==============

Use the following instructions to setup your Eclipse IDE with the standard code formatting and cleanup rules.

Download Code Formatting xml files
-------------------------------------------
* Use the settings/eclipse-formatter.xml and settings/eclipse-cleanup.xml files based on the appropriate version of eclipse you use

Code formatting settings
--------------------------------

* Open Eclipse > Preferences...
* Open Java > Code Style > Formatter
* Click Import...
* Find eclipse-formatter.xml
* Open

Code cleanup settings
------------------------------

* Open Eclipse > Preferences...
* Open Java > Code Style > Clean Up
* Click Import...
* Find eclipse-cleanup.xml
* Open

Auto-save actions
------------------------

* Open Eclipse > Preferences...
* Open Java > Editor > Save Actions
* Select "Perform the selected actions on setup"
* Select "Format source code"
* Select "Format all lines"
* Select "Organize imports"
* Select "Additional actions"
* Click "Configure..."
* Select the following settings:
    * Code Organizing > Remove trailing whitespace > All lines
    * Code Organizing > Correct indentation
    * Code Style > Use blocks in if/while/for/do statements > Always
    * Code Style > Use modifier 'final' where possible > Parameters
    * Code Style > Use modifier 'final' where possible > Local variables
    * Code Style > Functional interface instances > Convert functional interface instances > Use lambda where possible
    * Member Accesses > Use 'this' qualifier for field accesses > Always
    * Member Accesses > Use 'this' qualifier for method accesses > Always
    * Missing Code > Add missing Annotations > @Override > Implementations of interface methods (1.6 or higher)
    * Missing Code > Add missing Annotations > @Deprecated
    * Unnecessary Code > Remove unused imports
    * Unnecessary Code > Remove unnecessary casts
    * Unnecessary Code > Remove unnecessary $NON-NLS tags


Other
-------

* Open Eclipse > Preferences...
* Open General > Editors > Text Editors
    * Set "Displayed tab width" = 4
    * Set "Insert spaces for tabs"
* Open XML > XML Files > Editor
    * Set "Indent using spaces"
    * Set "Indention size" = 4

Apply Formatting style to project
-------
* Right Click on Project
* Source -> Format
