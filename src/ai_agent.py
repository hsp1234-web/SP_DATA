import json
import logging
import time
import random
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Tuple

# AI Agent Logger
# Logger setup within the module
ai_agent_logger = logging.getLogger(f"project_logger.{__name__}")
if not ai_agent_logger.handlers and not logging.getLogger().hasHandlers(): # Check root logger too
    ai_agent_logger.addHandler(logging.NullHandler())
    ai_agent_logger.debug(f"Logger for {__name__} (ai_agent module) configured with NullHandler for atomic script.")

class BaseAIAgent(ABC):
    """
    AI 代理的抽象基類。
    """
    def __init__(self, config: Optional[Dict[str, Any]] = None, logger_instance: Optional[logging.Logger] = None):
        self.config = config if config is not None else {}
        if logger_instance:
            self.logger = logger_instance
        else:
            # Fallback to module-level logger if no specific instance is provided
            self.logger = logging.getLogger(f"project_logger.{self.__class__.__name__}")
            if not self.logger.handlers and not logging.getLogger().hasHandlers():
                 self.logger.addHandler(logging.NullHandler()) # Ensure it has a handler if used directly
                 self.logger.debug(f"Instance logger for {self.__class__.__name__} (BaseAIAgent) using NullHandler for atomic script.")


    @abstractmethod
    def get_decision(self, market_brief_json: str) -> Tuple[Optional[str], Optional[str]]:
        """
        接收市場簡報 JSON，返回 AI 的決策文本和錯誤訊息。

        Args:
            market_brief_json (str): 市場簡報的 JSON 字串。

        Returns:
            Tuple[Optional[str], Optional[str]]: (ai_response_text, error_message)
            成功時，ai_response_text 是 AI 的回應，error_message 為 None。
            失敗時，ai_response_text 為 None，error_message 包含錯誤信息。
        """
        pass

class MockAIAgent(BaseAIAgent):
    """
    一個模擬的 AI 代理，用於開發和測試。
    返回一個固定的、結構化的 JSON 字串。
    """
    def __init__(self, config: Optional[Dict[str, Any]] = None, logger_instance: Optional[logging.Logger] = None):
        super().__init__(config, logger_instance)
        self.mock_response_template = {
            "strategy_summary": "基於模擬數據，建議採取觀望策略。",
            "key_factors": ["市場波動性指標模擬值中等", "利率預期模擬值穩定"],
            "confidence_score": 0.75,
            "raw_input_received": "" # Will be populated with the input
        }
        self.requests_config = self.config.get('requests_config', {}) if self.config else {}
        self.simulate_network_latency_max_sec = self.config.get('ai_agent_mock_config', {}).get('simulate_network_latency_max_sec', 0.1)
        self.simulate_failure_rate = self.config.get('ai_agent_mock_config', {}).get('simulate_failure_rate', 0.0) # 0.0 means no failures

    def get_decision(self, market_brief_json: str) -> Tuple[Optional[str], Optional[str]]:
        self.logger.info(f"MockAIAgent received market brief (first 100 chars): {market_brief_json[:100]}...")

        # Simulate network latency
        latency = random.uniform(0, self.simulate_network_latency_max_sec)
        self.logger.debug(f"MockAIAgent: Simulating network latency of {latency:.3f} seconds.")
        time.sleep(latency)

        # Simulate potential failure
        if random.random() < self.simulate_failure_rate:
            error_message = "MockAIAgent: Simulated AI decision failure."
            self.logger.error(error_message)
            # Simulate a malformed or error response from AI
            malformed_response = "{\"error\": \"Simulated AI processing error\", \"details\": \"Failed to generate strategy.\"}"
            return malformed_response, error_message # Return malformed JSON as AI response text on error

        # Simulate API call with retries (though for mock, it's mostly for show)
        max_retries = self.requests_config.get('max_retries', 1) # Default to 1 for mock if not configured
        base_backoff = self.requests_config.get('base_backoff_seconds', 0.1)

        for attempt in range(max_retries):
            try:
                # Simulate a successful call after some attempts if retries > 1
                if attempt > 0: # Simulate some processing for retries
                    self.logger.info(f"MockAIAgent: Simulating retry attempt {attempt + 1}/{max_retries}")
                    time.sleep(base_backoff * (2 ** attempt) * random.uniform(0.8, 1.2)) # Exponential backoff with jitter

                # Construct the mock response
                current_response = self.mock_response_template.copy()
                current_response["raw_input_received"] = market_brief_json # Include the input for verification

                ai_response_str = json.dumps(current_response, ensure_ascii=False, indent=2)
                self.logger.info("MockAIAgent successfully generated a mock decision.")
                return ai_response_str, None # Success

            except Exception as e: # Should not happen with json.dumps unless data is weird
                self.logger.error(f"MockAIAgent: Error during decision generation (attempt {attempt + 1}): {e}", exc_info=True)
                if attempt == max_retries - 1:
                    return None, f"MockAIAgent: Failed after {max_retries} attempts: {e}"
                # Continue to next retry attempt

        # Should be unreachable if max_retries >= 1
        return None, "MockAIAgent: Max retries reached without returning a response (unexpected)."

if __name__ == '__main__':
    # Basic logger for standalone testing of ai_agent.py
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
                            handlers=[logging.StreamHandler()]) # Log to console for tests

    test_logger_ai_agent = logging.getLogger("AIAgentTestRun_Atomic")
    if not test_logger_ai_agent.handlers: # Avoid adding handlers multiple times
        ch_ai_agent = logging.StreamHandler()
        ch_ai_agent.setFormatter(logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(message)s'))
        test_logger_ai_agent.addHandler(ch_ai_agent)
        test_logger_ai_agent.propagate = False

    test_logger_ai_agent.info("--- Starting AIAgent Test ---")

    # Test MockAIAgent
    mock_agent_config = {
        'requests_config': {'max_retries': 2, 'base_backoff_seconds': 0.05},
        'ai_agent_mock_config': {
            'simulate_network_latency_max_sec': 0.05,
            'simulate_failure_rate': 0.0 # Test with no failures first
        }
    }
    mock_agent = MockAIAgent(config=mock_agent_config, logger_instance=test_logger_ai_agent)

    sample_brief = {"briefing_date": "2023-10-26", "dealer_stress_index": {"current_value_description": "中度緊張"}}
    sample_brief_json = json.dumps(sample_brief)

    test_logger_ai_agent.info(f"Testing MockAIAgent with input: {sample_brief_json}")
    response_text, error = mock_agent.get_decision(sample_brief_json)

    if error:
        test_logger_ai_agent.error(f"MockAIAgent test failed with error: {error}")
    elif response_text:
        test_logger_ai_agent.info(f"MockAIAgent test successful. Response:\n{response_text}")
        try:
            response_data = json.loads(response_text)
            assert "strategy_summary" in response_data
            assert "key_factors" in response_data
            test_logger_ai_agent.info("Mock response content validated.")
        except json.JSONDecodeError:
            test_logger_ai_agent.error("MockAIAgent response was not valid JSON.")
        except AssertionError:
            test_logger_ai_agent.error("MockAIAgent response missing expected keys.")
    else:
        test_logger_ai_agent.error("MockAIAgent test failed: No response text and no error message.")

    # Test simulated failure
    test_logger_ai_agent.info("\n--- Testing MockAIAgent with simulated failure ---")
    mock_agent_config_failure = {
        'requests_config': {'max_retries': 1}, # Fail faster
        'ai_agent_mock_config': {
            'simulate_network_latency_max_sec': 0.01,
            'simulate_failure_rate': 1.0 # Always fail
        }
    }
    mock_agent_fail = MockAIAgent(config=mock_agent_config_failure, logger_instance=test_logger_ai_agent)
    response_text_fail, error_fail = mock_agent_fail.get_decision(sample_brief_json)

    if error_fail:
        test_logger_ai_agent.info(f"MockAIAgent simulated failure test OK. Error: {error_fail}")
        if response_text_fail:
            test_logger_ai_agent.info(f"AI response text on failure: {response_text_fail}")
            try:
                json.loads(response_text_fail) # Check if it's valid JSON, even if error content
                test_logger_ai_agent.info("AI response on failure is valid JSON.")
            except json.JSONDecodeError:
                 test_logger_ai_agent.warning("AI response on failure was not valid JSON.")
    else:
        test_logger_ai_agent.error("MockAIAgent simulated failure test FAILED: No error message returned.")


    test_logger_ai_agent.info("--- AIAgent Test Finished ---")
