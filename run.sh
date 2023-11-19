export DAGSTER_HOME=$(pwd)/dagster_home
mkdir -p $DAGSTER_HOME
echo $DAGSTER_HOME
export ENV_TYPE=dev
dagster dev -h 0.0.0.0 -f $(pwd)/dag.py
