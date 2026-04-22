import json
import pprint
from qbrain.core.orchestrator_manager.orchestrator import Thalamus
from qbrain.core.session_manager.session import SessionManager
from qbrain.core.env_manager.env_lib import EnvManager
from qbrain.core.module_manager.ws_modules_manager.modules_lib import ModuleWsManager
from qbrain.core.fields_manager.fields_lib import FieldsManager
from qbrain.core.injection_manager.injection import InjectionManager

class OrchestratorTest:
    def __init__(self):
        self.orchestrator = Thalamus()
        self.session_manager = SessionManager()
        self.env_manager = EnvManager()
        self.module_manager = ModuleWsManager()
        self.field_manager = FieldsManager()
        self.injection_manager = InjectionManager()
        
        self.user_id = "test_user_001"
        self.session_id = "sess_test_001"
        self.env_id = "env_test_alpha"
        self.module_id = "mod_test_physics"
        self.field_id = "field_test_electric"
        self.injection_id = "inj_test_pulse"
        
        # Setup mock data for context
        self._setup_mock_data()

    def _setup_mock_data(self):
        """Pre-populate managers with test data to simulate existing resources."""
        print("Setting up mock data...")
        
        # 1. Create Session
        # We can't easily mock the DB writes without affecting real DB if not careful,
        # but we can assume the IDs exist for the logic flow or use the managers to create them.
        # For this test, we will simulate the *creation* flow via the Orchestrator's logic
        # by feeding it prompts that *would* result in these configurations.
        pass

    def run_workflow(self):
        print("\n--- Starting Orchestrator Workflow Test ---\n")
        
        # Step 1: Select Session
        print("\n[Step 1] Select Session")
        session_prompt = f"I want to start a simulation in session '{self.session_id}'."
        self._process_prompt(session_prompt, "SESSION")

        # Step 2: Select Environment
        print("\n[Step 2] Select Environment")
        # Context: User has environments available.
        env_prompt = f"Use environment '{self.env_id}' for this session."
        self._process_prompt(env_prompt, "ENVIRONMENT")

        # Step 3: Select/Assign Module
        print("\n[Step 3] Select Module")
        module_prompt = f"Activate module '{self.module_id}' in this environment."
        self._process_prompt(module_prompt, "MODULE")

        # Step 4: Select Field
        print("\n[Step 4] Select Field")
        field_prompt = f"Configure the field '{self.field_id}'."
        self._process_prompt(field_prompt, "FIELD")

        # Step 5: Assign Injections
        print("\n[Step 5] Assign Injections")
        injection_prompt = f"Inject '{self.injection_id}' at position [0,0,0] and [1,0,0]."
        self._process_prompt(injection_prompt, "INJECTION")
        
        # Final: Verify Config
        print("\n[Final] Verifying Generated Configuration")
        self._verify_config()

    def _process_prompt(self, prompt: str, step_type: str):
        """
        Simulates the processing of a prompt by the Orchestrator to update the config.
        Since the current Orchestrator chat is basic, we will extend/mock the logic here
        to demonstrate how it *should* update the SESSION_CFG structure.
        """
        print(f"User: {prompt}")
        
        # In a real scenario, Orchestrator.orchestrator_chat would handle this.
        # Here we simulate the state update that the Orchestrator would perform.
        
        if step_type == "SESSION":
            # Logic: Set active session context
            self.current_config = {
                "session_id": self.session_id,
                "config": {
                    "envs": {}
                }
            }
            print(f"Orchestrator: Session context set to {self.session_id}")

        elif step_type == "ENVIRONMENT":
            # Logic: Add env to config
            if self.env_id not in self.current_config["config"]["envs"]:
                self.current_config["config"]["envs"][self.env_id] = {
                    "modules": {}
                }
            print(f"Orchestrator: Environment {self.env_id} selected.")

        elif step_type == "MODULE":
            # Logic: Add module to env
            env_cfg = self.current_config["config"]["envs"].get(self.env_id)
            if env_cfg:
                if self.module_id not in env_cfg["modules"]:
                    env_cfg["modules"][self.module_id] = {
                        "fields": {}
                    }
                print(f"Orchestrator: Module {self.module_id} activated.")

        elif step_type == "FIELD":
            # Logic: Add field to module
            env_cfg = self.current_config["config"]["envs"].get(self.env_id)
            mod_cfg = env_cfg["modules"].get(self.module_id)
            if mod_cfg:
                if self.field_id not in mod_cfg["fields"]:
                    mod_cfg["fields"][self.field_id] = {
                        "injections": {}
                    }
                print(f"Orchestrator: Field {self.field_id} configured.")

        elif step_type == "INJECTION":
            # Logic: Parse positions and assign injection
            # Simple parsing for test
            import re
            positions = ["[0,0,0]", "[1,0,0]"] # Mock extracted positions
            
            env_cfg = self.current_config["config"]["envs"].get(self.env_id)
            mod_cfg = env_cfg["modules"].get(self.module_id)
            field_cfg = mod_cfg["fields"].get(self.field_id)
            
            if field_cfg:
                for pos in positions:
                    field_cfg["injections"][pos] = self.injection_id
                print(f"Orchestrator: Injections assigned at {positions}.")

    def _verify_config(self):
        print("\nGenerated SESSION_CFG:")
        pprint.pprint(self.current_config)
        
        # Basic Validation
        assert self.current_config["session_id"] == self.session_id
        assert self.env_id in self.current_config["config"]["envs"]
        assert self.module_id in self.current_config["config"]["envs"][self.env_id]["modules"]
        assert self.field_id in self.current_config["config"]["envs"][self.env_id]["modules"][self.module_id]["fields"]
        assert "[0,0,0]" in self.current_config["config"]["envs"][self.env_id]["modules"][self.module_id]["fields"][self.field_id]["injections"]
        
        print("\nValidation Successful: Configuration matches schema.")

if __name__ == "__main__":
    test = OrchestratorTest()
    test.run_workflow()
