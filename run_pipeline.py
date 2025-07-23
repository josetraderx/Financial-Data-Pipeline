import os
import sys
import json
import logging
from datetime import datetime, timedelta

# Crear carpeta de logs si no existe
def ensure_log_dir(log_dir):
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

# Configuración de logging estructurado
def setup_logging(log_dir):
    ensure_log_dir(log_dir)
    log_file = os.path.join(log_dir, 'run_pipeline.log')
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='a', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

# Cargar configuración desde archivo JSON
def load_config(config_path):
    with open(config_path, 'r') as f:
        return json.load(f)

# Selección de pipeline (por ahora solo 'crypto')
def get_pipeline(pipeline_name):
    if pipeline_name == 'crypto':
        from data_etl.pipelines.crypto_pipeline import CryptoPipeline
        from data_etl.pipelines.config_manager import PipelineConfig
        return CryptoPipeline, PipelineConfig
    # Aquí puedes agregar más pipelines en el futuro
    else:
        raise ValueError(f"Pipeline '{pipeline_name}' is not supported yet.")


def main():
    config_path = os.path.join('config', 'pipeline_config.json')
    log_dir = os.path.join('logs')
    setup_logging(log_dir)
    logging.info('Starting pipeline orchestration.')

    try:
        config = load_config(config_path)
        pipeline_name = config.get('pipeline')
        pipeline_params = config.get('pipeline_config', {})
        # Agregar fechas de los últimos 5 años si no están presentes
        if 'start_date' not in pipeline_params or 'end_date' not in pipeline_params:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=5*365)
            pipeline_params['start_date'] = start_date.strftime('%Y-%m-%d')
            pipeline_params['end_date'] = end_date.strftime('%Y-%m-%d')
        logging.info(f"Selected pipeline: {pipeline_name}")
        logging.info(f"Pipeline parameters: {pipeline_params}")

        CryptoPipeline, PipelineConfig = get_pipeline(pipeline_name)
        pipeline_config = PipelineConfig(config_path)
        pipeline = CryptoPipeline(pipeline_config.get())
        results = pipeline.run_pipeline(pipeline_params)
        logging.info('Pipeline executed successfully.')
        logging.info(f'Results: {results}')
    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}", exc_info=True)
        sys.exit(1)
    logging.info('Pipeline orchestration finished.')

if __name__ == '__main__':
    main() 