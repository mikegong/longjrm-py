import os
import logging
from longjrm.logger import Logger
from dotenv import load_dotenv, find_dotenv
from longjrm.file_loader import load_ini, load_json


# Initialize environment
# All files here use absolute paths

try:
    # Load key initial environment variables from local .env file, if USE_DOTENV is set
    # In production environment, suggest setting up those key initial env variables in OS,
    # including JRM_PY_ENV, JRM_PY_ENV_PATH
    if os.getenv('USE_DOTENV') == 'true':
        dotenv_path = find_dotenv(os.getenv('DOTENV_PATH'))
        if os.path.exists(dotenv_path):
            _ = load_dotenv(dotenv_path)

    env_path = os.getenv('JRM_PY_ENV_PATH')
    config_ini = 'config_' + os.getenv('JRM_PY_ENV', 'dev') + '.ini'
    dbinfos_json = 'dbinfos_' + os.getenv('JRM_PY_ENV', 'dev') + '.json'

    # JRM environment is loaded as following module level variables
    logger = Logger(os.getenv('LOG_FILE'), logging.INFO, os.getenv('APP')).getlog()
    config = load_ini(os.path.join(env_path, config_ini))
    dbinfos = load_json(os.path.join(env_path, dbinfos_json))
    db_lib_map = load_json(os.path.join(env_path, 'db_lib_map.json'))

    logger.info("JRM environment initialized")

except Exception as e:
    logger.error(f"JRM environment initialization exception: {e}")
    raise

