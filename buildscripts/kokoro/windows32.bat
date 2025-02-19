@rem ##########################################################################
@rem
@rem Runs tests and then builds artifacts to %WORKSPACE%\artifacts\
@rem
@rem ##########################################################################

type c:\VERSION

@rem Enter repo root
cd /d %~dp0\..\..

set WORKSPACE=T:\src\github\grpc-java
set ESCWORKSPACE=%WORKSPACE:\=\\%


@rem Clear JAVA_HOME to prevent a different Java version from being used
set JAVA_HOME=
set PATH=C:\Program Files\OpenJDK\openjdk-11.0.12_7\bin;%PATH%

for /f "usebackq delims=" %%i in (`vswhere -version "[16.0,17.0)" -property installationPath`) do (
  set VSDIR=%%i
)

mkdir grpc-java-helper32
cd grpc-java-helper32
dir "%VSDIR%"
dir "%VSDIR%\BuildTools\"
dir "%VSDIR%\BuildTools\VC\"
dir "%VSDIR%\BuildTools\VC\Axiliary\"
dir "%VSDIR%\BuildTools\VC\Axiliary\Build\"
call "%VSDIR%\Common7\Tools\VsDevCmd.bat" -arch=x86 || exit /b 1
echo on
cmake --version
call "%WORKSPACE%\buildscripts\make_dependencies.bat" || exit /b 1

cd "%WORKSPACE%"

SET TARGET_ARCH=x86_32
SET FAIL_ON_WARNINGS=true
SET VC_PROTOBUF_LIBS=%ESCWORKSPACE%\\grpc-java-helper32\\protobuf-%PROTOBUF_VER%\\build\\Release
SET VC_PROTOBUF_INCLUDE=%ESCWORKSPACE%\\grpc-java-helper32\\protobuf-%PROTOBUF_VER%\\build\\include
SET GRADLE_FLAGS=-PtargetArch=%TARGET_ARCH% -PfailOnWarnings=%FAIL_ON_WARNINGS% -PvcProtobufLibs=%VC_PROTOBUF_LIBS% -PvcProtobufInclude=%VC_PROTOBUF_INCLUDE% -PskipAndroid=true
SET GRADLE_OPTS="-Dorg.gradle.jvmargs='-Xmx1g'"

cmd.exe /C "%WORKSPACE%\gradlew.bat %GRADLE_FLAGS% build"
set GRADLEEXIT=%ERRORLEVEL%

@rem Rename test results .xml files to format parsable by Kokoro
@echo off
for /r %%F in (TEST-*.xml) do (
  mkdir "%%~dpnF"
  move "%%F" "%%~dpnF\sponge_log.xml" >NUL
)
@echo on

IF NOT %GRADLEEXIT% == 0 (
  exit /b %GRADLEEXIT%
)

@rem make sure no daemons have any files open
cmd.exe /C "%WORKSPACE%\gradlew.bat --stop"

cmd.exe /C "%WORKSPACE%\gradlew.bat  %GRADLE_FLAGS% -Dorg.gradle.parallel=false -PrepositoryDir=%WORKSPACE%\artifacts clean grpc-compiler:build grpc-compiler:publish" || exit /b 1
