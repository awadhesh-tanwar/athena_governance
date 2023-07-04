export ENTITY_NAME='payufin'
export ENV='prod'
export SERVICE_NAME=' governance-apis'
export HASHICORP_URL='https://payufin.atlassian.net/browse/DO-16899#:~:text=https%3A//hvault%2Dpf%2Dprod%2Dcluster.internal.payufin.io'

echo $PATH
gunicorn --bind 0.0.0.0:8000 app.wsgi --workers 10