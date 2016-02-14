#!/bin/bash
#Source: https://www.michaelmiklis.de/read-netatmo-weather-station-data-via-script/
 
listDevices() {
    # ------------------------------------------------------
    # Help
    # ------------------------------------------------------
    # usage: listdevices <USER> <PASSWORD>
    #
    # USER + PASSWORD -> your NetAtmo Website login
 
    # ------------------------------------------------------
    # Parsing Arguments
    # ------------------------------------------------------
    USER=$1
    PASS=$2
 
 
    # ------------------------------------------------------
    # Define some constants
    # ------------------------------------------------------
    URL_LOGIN="https://auth.netatmo.com/de-DE/access/login"
    API_GETMEASURECSV="https://my.netatmo.com/api/devicelist"
    SESSION_COOKIE="cookie_sess.txt"
    AUTH_COOKIE="cookie_auth.txt"
 
 
    # ------------------------------------------------------
    # URL encode the user entered parameters
    # ------------------------------------------------------
    USER="$(urlencode $USER)"
    PASS="$(urlencode $PASS)"
    
    
     
    # ------------------------------------------------------
    # Now let's fetch the data
    # ------------------------------------------------------
 
    # first we need to get a valid session cookie
    curl --silent -c $AUTH_COOKIE $URL_LOGIN  > /dev/null
 
    # then extract the ID from the authentication cookie
    SESS_ID="$(cat $AUTH_COOKIE | grep netatmocomci_csrf_cookie_na | cut -f7)"
 
    # and now we can login using cookie, id, user and password
    curl --silent -d "ci_csrf_netatmo=$SESS_ID&mail=$USER&pass=$PASS&log_submit=LOGIN" -b $AUTH_COOKIE -c $SESSION_COOKIE  $URL_LOGIN > /dev/null
 
    # next we extract the access_token from the session cookie
    ACCESS_TOKEN="$(cat $SESSION_COOKIE | grep netatmocomaccess_token | cut -f7)"
 
    # build the POST data
    PARAM="access_token=$ACCESS_TOKEN"
 
    # now download json data   
    curl --silent -d $PARAM $API_GETMEASURECSV
 
    # clean up
    rm $SESSION_COOKIE
    rm $AUTH_COOKIE
}
 
#____________________________________________________________________________________________________________________________________
 
urlencode() {
    # ------------------------------------------------------
    # urlencode function from mrubin
    # https://gist.github.com/mrubin
    #
    # usage: urlencode <string>
    # ------------------------------------------------------
    local length="${#1}"
 
    for (( i = 0; i < length; i++ )); do
        local c="${1:i:1}"
 
        case $c in [a-zA-Z0-9.~_-])
            printf "$c" ;;
            *) printf '%%%02X' "'$c"
            esac
    done
}
 
#____________________________________________________________________________________________________________________________________
 
listDevices $1 $2