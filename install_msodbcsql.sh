# !/bin/sh

case $(uname -m) in
    x86_64) architecture="amd64" ;;
    arm64) architecture="arm64" ;;
    *) architecture="unsupported" ;;
esac
if [[ "unsupported" == "$architecture" ]];
then
    echo "Alpine architecture $(uname -m) is not currently supported.";
    exit;
fi

#Download the desired package(s)
curl -O https://download.microsoft.com/download/0b3d5518-b4a7-4a2b-afc7-7ee9e967f93c/msodbcsql18_18.6.2.1-1_$architecture.apk
curl -O https://download.microsoft.com/download/cad0d30f-b9b1-4765-a011-81d8a66c8b8d/mssql-tools18_18.6.2.1-1_$architecture.apk

#(Optional) Verify signature, if 'gpg' is missing install it using 'apk add gnupg':
curl -O https://download.microsoft.com/download/0b3d5518-b4a7-4a2b-afc7-7ee9e967f93c/msodbcsql18_18.6.2.1-1_$architecture.sig
curl -O https://download.microsoft.com/download/cad0d30f-b9b1-4765-a011-81d8a66c8b8d/mssql-tools18_18.6.2.1-1_$architecture.sig

curl https://packages.microsoft.com/keys/microsoft.asc | gpg --import -
gpg --verify msodbcsql18_18.6.2.1-1_$architecture.sig msodbcsql18_18.6.2.1-1_$architecture.apk
gpg --verify mssql-tools18_18.6.2.1-1_$architecture.sig mssql-tools18_18.6.2.1-1_$architecture.apk

#Install the package(s)
apk add --allow-untrusted msodbcsql18_18.6.2.1-1_$architecture.apk
apk add --allow-untrusted mssql-tools18_18.6.2.1-1_$architecture.apk

# Source : https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver17&tabs=alpine18-install%2Calpine17-install%2Cdebian8-install%2Credhat7-13-install%2Crhel7-offline
