import threading
from typing import Optional, Dict, Any, Tuple
from collections import deque

DECAY_FACTOR_MIN = 0.8
DECAY_FACTOR_MAX = 1.0
RESET_INTERVAL_MIN = 10000
RESET_INTERVAL_MAX = 200000

MAX_DECAY_DELTA = 0.02
MAX_DECAY_DELTA_AGGRESSIVE = 0.10
MAX_RESET_INTERVAL_DELTA_PERCENT = 0.20
MAX_RESET_INTERVAL_DELTA_PERCENT_AGGRESSIVE = 0.40

ROLLBACK_HIT_RATIO_DECREASE_THRESHOLD = 0.01
HIT_RATIO_HISTORY_SIZE = 3


class OptimizationSafetyGuard:
    def __init__(self):
        self.lock = threading.RLock()

        self.last_stable_config = None
        self.last_applied_config = None

        self.hit_ratio_history = deque(maxlen=HIT_RATIO_HISTORY_SIZE)

        self.last_stable_config_snapshot_time = None

        self.in_rollback_state = False
    
    def validate_plan(
        self,
        plan: Dict[str, Any],
        current_decay_factor: Optional[float],
        current_reset_interval: int,
        adaptation_state=None
    ) -> Tuple[bool, Optional[Dict[str, Any]], str, bool]:
        with self.lock:
            tinylfu_control = plan.get("tinylfu_control", {})
            if not tinylfu_control:
                return False, None, "Missing tinylfu_control in plan", False

            decay_factor = tinylfu_control.get("decay_factor")
            reset_interval = tinylfu_control.get("reset_interval")
            doorkeeper_bias = tinylfu_control.get("doorkeeper_bias")

            is_aggressive_state = adaptation_state and adaptation_state.value in ["STABLE_BUT_INEFFECTIVE", "UNSTABLE"]

            if decay_factor is not None:
                if not (DECAY_FACTOR_MIN <= decay_factor <= DECAY_FACTOR_MAX):
                    return False, None, f"decay_factor {decay_factor} out of bounds [{DECAY_FACTOR_MIN}, {DECAY_FACTOR_MAX}]", False

                validated_control = {}
                validated_control["decay_factor"] = decay_factor

                if reset_interval is not None:
                    if not (RESET_INTERVAL_MIN <= reset_interval <= RESET_INTERVAL_MAX):
                        return False, None, f"reset_interval {reset_interval} out of bounds [{RESET_INTERVAL_MIN}, {RESET_INTERVAL_MAX}]", False
                    validated_control["reset_interval"] = reset_interval

                if doorkeeper_bias is not None:
                    if not isinstance(doorkeeper_bias, dict):
                        return False, None, f"doorkeeper_bias must be a dict, got {type(doorkeeper_bias)}", False
                    validated_control["doorkeeper_bias"] = doorkeeper_bias

                validated_plan = plan.copy()
                validated_plan["tinylfu_control"] = validated_control
                reason = "Validation passed (decay_factor change - safety guard bypassed)"
                if is_aggressive_state:
                    reason += f" - Aggressive state ({adaptation_state.value}) allows multiple parameters"
                return True, validated_plan, reason, False

            param_count = sum(1 for p in [decay_factor, reset_interval, doorkeeper_bias] if p is not None)

            if param_count > 1:
                if is_aggressive_state:
                    print(f"[SAFETY_GUARD] Allowing {param_count} parameter changes in {adaptation_state.value} state (coordinated shock enabled)")
                else:
                    return False, None, f"Too many parameter changes: {param_count} (max 1 allowed)", False

            validated_control = {}
            was_clamped = False

            if reset_interval is not None:
                if not (RESET_INTERVAL_MIN <= reset_interval <= RESET_INTERVAL_MAX):
                    return False, None, f"reset_interval {reset_interval} out of bounds [{RESET_INTERVAL_MIN}, {RESET_INTERVAL_MAX}]", False

                original_reset = reset_interval
                delta_percent = abs(reset_interval - current_reset_interval) / current_reset_interval
                if delta_percent > MAX_RESET_INTERVAL_DELTA_PERCENT:
                    if reset_interval > current_reset_interval:
                        reset_interval = int(current_reset_interval * (1 + MAX_RESET_INTERVAL_DELTA_PERCENT))
                    else:
                        reset_interval = int(current_reset_interval * (1 - MAX_RESET_INTERVAL_DELTA_PERCENT))
                    reset_interval = max(RESET_INTERVAL_MIN, min(RESET_INTERVAL_MAX, reset_interval))
                    was_clamped = True

                validated_control["reset_interval"] = reset_interval

            if doorkeeper_bias is not None:
                if not isinstance(doorkeeper_bias, dict):
                    return False, None, f"doorkeeper_bias must be a dict, got {type(doorkeeper_bias)}", False
                validated_control["doorkeeper_bias"] = doorkeeper_bias

            validated_plan = plan.copy()
            validated_plan["tinylfu_control"] = validated_control

            reason = "Validation passed"
            if was_clamped:
                reason += " (values clamped to meet delta constraints)"

            return True, validated_plan, reason, was_clamped
    
    def update_hit_ratio(self, hit_ratio: float, snapshot_time: Optional[float] = None):
        with self.lock:
            self.hit_ratio_history.append({
                "hit_ratio": hit_ratio,
                "timestamp": snapshot_time
            })
    
    def check_rollback_condition(self) -> Tuple[bool, Optional[Dict[str, Any]]]:
        with self.lock:
            if len(self.hit_ratio_history) < 2:
                return False, None

            last_two = list(self.hit_ratio_history)[-2:]

            decreases = []
            for i in range(len(last_two) - 1):
                prev_hr = last_two[i]["hit_ratio"]
                curr_hr = last_two[i + 1]["hit_ratio"]
                decrease = prev_hr - curr_hr
                decreases.append(decrease)

            if len(decreases) >= 2:
                if decreases[-2] > ROLLBACK_HIT_RATIO_DECREASE_THRESHOLD and \
                   decreases[-1] > ROLLBACK_HIT_RATIO_DECREASE_THRESHOLD:
                    if self.last_stable_config is not None:
                        self.in_rollback_state = True
                        return True, self.last_stable_config.copy()

            return False, None
    
    def save_stable_config(
        self,
        decay_factor: Optional[float],
        reset_interval: int,
        doorkeeper_bias: Optional[Dict[str, Any]],
        snapshot_time: Optional[float] = None
    ):
        with self.lock:
            if self.in_rollback_state:
                return

            self.last_stable_config = {
                "decay_factor": decay_factor,
                "reset_interval": reset_interval,
                "doorkeeper_bias": doorkeeper_bias.copy() if doorkeeper_bias else None,
                "timestamp": snapshot_time
            }
            self.last_stable_config_snapshot_time = snapshot_time
    
    def update_applied_config(
        self,
        decay_factor: Optional[float],
        reset_interval: int,
        doorkeeper_bias: Optional[Dict[str, Any]]
    ):
        with self.lock:
            self.last_applied_config = {
                "decay_factor": decay_factor,
                "reset_interval": reset_interval,
                "doorkeeper_bias": doorkeeper_bias.copy() if doorkeeper_bias else None
            }

            if self.in_rollback_state:
                self.in_rollback_state = False
    
    def get_current_config(self) -> Dict[str, Any]:
        with self.lock:
            if self.last_applied_config:
                return self.last_applied_config.copy()
            return {
                "decay_factor": None,
                "reset_interval": 100000,
                "doorkeeper_bias": None
            }
    
    def get_last_stable_config(self) -> Optional[Dict[str, Any]]:
        with self.lock:
            if self.last_stable_config:
                return self.last_stable_config.copy()
            return None

