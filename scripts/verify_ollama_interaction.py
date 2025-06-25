import requests
import json
import logging

# 配置日誌記錄
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 從 config.yaml 中讀取的 Ollama API 端點 (理想情況下，應從配置文件加載)
OLLAMA_API_BASE_URL = "http://localhost:11434/api" # 您的 config.yaml 中的 ollama_api_base_url
MODEL_NAME = "llama3:8b-instruct-q4_K_M" # 您的 config.yaml 中的 local_model_name

def check_ollama_server_status():
    """檢查 Ollama 服務是否正在運行"""
    try:
        response = requests.get(OLLAMA_API_BASE_URL.replace("/api", "/"), timeout=5) # 檢查根路徑
        if response.status_code == 200 and "Ollama is running" in response.text:
            logger.info("Ollama server is running.")
            return True
        else:
            logger.error(f"Ollama server status check failed. Status: {response.status_code}, Response: {response.text[:100]}")
            return False
    except requests.exceptions.ConnectionError:
        logger.error("Connection to Ollama server failed. Is Ollama running?")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred while checking Ollama status: {e}")
        return False

def list_local_models():
    """列出 Ollama 本地可用的模型"""
    try:
        response = requests.get(f"{OLLAMA_API_BASE_URL}/tags")
        response.raise_for_status()
        models = response.json().get("models", [])
        logger.info("Available local models:")
        if not models:
            logger.info("  No local models found.")
        for model in models:
            logger.info(f"  - {model['name']} (Size: {model['size'] // (1024**3)} GB)")
        return [m['name'] for m in models]
    except requests.exceptions.RequestException as e:
        logger.error(f"Error listing local models: {e}")
        return []

def simple_chat_test(model_name: str):
    """對指定的模型進行一個簡單的問答測試"""
    logger.info(f"Performing simple chat test with model: {model_name}")

    payload = {
        "model": model_name,
        "prompt": "你好！請用繁體中文簡短介紹一下你自己。",
        "stream": False, # 為了簡單起見，不使用流式傳輸
        "options": {
            "temperature": 0.7
        }
    }

    try:
        response = requests.post(f"{OLLAMA_API_BASE_URL}/generate", json=payload)
        response.raise_for_status()

        response_data = response.json()

        logger.info(f"Raw response from {model_name}:")
        # 打印部分原始回應以供調試
        # logger.info(json.dumps(response_data, indent=2, ensure_ascii=False))

        if "response" in response_data:
            ai_response = response_data["response"].strip()
            logger.info(f"AI ({model_name}) Response: {ai_response}")

            # 檢查回應是否為空或僅包含特殊字符
            if not ai_response or ai_response.isspace():
                logger.warning("AI response is empty or contains only whitespace.")
                return False
            return True
        else:
            logger.error(f"Unexpected response structure from {model_name}. 'response' field missing.")
            logger.error(f"Full response: {response_data}")
            return False

    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error during chat test with {model_name}: {e}")
        if e.response is not None:
            logger.error(f"Response content: {e.response.text}")
        return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Request exception during chat test with {model_name}: {e}")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred during chat test: {e}")
        return False

if __name__ == "__main__":
    logger.info("--- Ollama Interaction Verification Script ---")

    if not check_ollama_server_status():
        logger.info("Exiting due to Ollama server status check failure.")
    else:
        available_models = list_local_models()
        if not available_models:
            logger.warning(f"No local models found via Ollama API. Cannot proceed with chat test.")
            logger.warning(f"Please ensure you have pulled the model, e.g., 'ollama pull {MODEL_NAME}'")
        elif MODEL_NAME not in available_models:
            logger.warning(f"Target model '{MODEL_NAME}' not found in local Ollama models.")
            logger.warning(f"Available models are: {', '.join(available_models) if available_models else 'None'}")
            logger.warning(f"Please ensure you have pulled the model: 'ollama pull {MODEL_NAME}'")
        else:
            logger.info(f"Target model '{MODEL_NAME}' found locally.")
            if simple_chat_test(MODEL_NAME):
                logger.info(f"Successfully interacted with model '{MODEL_NAME}'. Basic Ollama setup seems OK.")
            else:
                logger.error(f"Failed to get a valid response from model '{MODEL_NAME}'. Check Ollama logs and model availability.")

    logger.info("--- Verification Script Finished ---")
