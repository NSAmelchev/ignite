::
:: Licensed to the Apache Software Foundation (ASF) under one or more
:: contributor license agreements.  See the NOTICE file distributed with
:: this work for additional information regarding copyright ownership.
:: The ASF licenses this file to You under the Apache License, Version 2.0
:: (the "License"); you may not use this file except in compliance with
:: the License.  You may obtain a copy of the License at
::
::      http://www.apache.org/licenses/LICENSE-2.0
::
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.
::

:: Runs tests on WSL (Windows Subsystem for Linux).
:: Ignite.NET must be built on Windows, but cross-platform tests can be run on Linux.

pushd .\

cd ..
powershell -executionpolicy remotesigned -file build.ps1 -skipJava -version 0.0.1-test

popd

bash -c "dotnet nuget locals all --clear"

echo Starting tests...

bash -c "dotnet test"

pause.