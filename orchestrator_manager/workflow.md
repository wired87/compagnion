---
description: QBrain Core Simulation Workflow - From Entry to Orchestration
---

# QBrain Core Simulation Workflow

This document outlines the end-to-end workflow for a user interacting with the QBrain Core simulation platform, from system initialization to the start of a simulation (Orchestration).

## 1. System Initialization (Server Start)
**Actor:** System / Admin
**Action:** Initialize Database and Standard Model.

*   **Table Initialization**: `QBrainTableManager.initialize_all_tables()` is called to ensure the BigQuery dataset `QBRAIN` and all required tables (`users`, `sessions`, `envs`, `modules`, etc.) exist with the correct schema.
*   **Standard Model (SM) Loading**: `SMManager.main(user_id="public")` checks if the "Standard Model" stack (default modules, fields, methods like `QuantumFieldUtils`) exists in the DB. If not, it builds the graph using `QFUtils`, extracts nodes/edges, and upserts them as a baseline for all users.

## 2. User Entry & Authentication
**Actor:** User
**Manager:** `UserManager`

1.  **Sign Up / Login**: User authenticates.
2.  **User Record**: `UserManager` ensures a `users` table entry exists.
3.  **SM Verification**: The system checks if the user has access to the Standard Model stack (often shared or linked).

## 3. Session Management
**Actor:** User
**Manager:** `SessionManager`

1.  **Create Session**: User initializes a new working session.
    *   `SessionManager.create_session(user_id)` generates a unique `session_id`.
    *   Context is established for subsequent actions.

## 4. Environment Setup
**Actor:** User
**Manager:** `EnvManager`

1.  **Create Environment**: User creates a specific simulation environment (e.g., "Test Simulation 1").
    *   `EnvManager.create_env(user_id, description)` returns an `env_id`.
2.  **Link to Session**: The environment is linked to the active `session_id`.

## 5. Module & Logic Configuration
**Actor:** User
**Managers:** `SMManager`, `ModuleWsManager`, `MethodManager`, `FieldsManager`, `ParamsManager`

1.  **Enable Standard Model**: User "enables" the Standard Model for their environment.
    *   `SMManager.enable_sm(user_id, session_id, env_id)` creates links between the standard modules/fields and the user's current environment.
    *   This populates the simulation with default physics/logic (e.g., Quantum Fields, Fermions).
2.  **Custom Modules (Optional)**:
    *   **Upload**: User uploads Python files/zips via `ModuleWsManager` / `FileManager`.
    *   **Extraction**: `FileManager` (using `RawModuleExtractor`) parses code to identify Classes (Modules), Functions (Methods), and Variables (Fields/Params).
    *   **Upsert**: The extracted entities are saved to their respective tables (`modules`, `methods`, `fields`) and linked to the `env_id`.

## 6. Parameter & Injection Setup
**Actor:** User
**Managers:** `ParamsManager`, `InjectionManager`

1.  **Configure Parameters**: User adjusts initial values (`ParamsManager`).
    *   Example: Setting coupling constants or initial field values.
2.  **Define Injections**: User defines energy/data injections (`InjectionManager`).
    *   These act as "stimuli" or initial conditions for the simulation.
    *   Saved to `injections` table and linked to `env_id`.

## 7. Simulation Start (Orchestration)
**Actor:** User -> Orchestrator
**Manager:** `Thalamus`

1.  **Trigger Orchestration**: User requests simulation start.
    *   `Thalamus.orchestrate(session_id, user_id)` is called.
2.  **Pattern Recognition**:
    *   The implied state of the simulation (configuration of all modules + fields + params + injections) is treated as a "Memory Pattern".
3.  **Hopfield Dynamics**:
    *    The Orchestrator applies energy minimization dynamics (conceptually based on Hopfield Networks).
    *   **Goal**: Find a stable equilibrium or "attractor state" for the simulation parameters.
    *   **Optimization**: It adjusts the simulation state from "noisy" initial inputs to a coherent, stable trajectory.
4.  **Execution**: The stabilized configuration is then effectively "run" (methods executed, fields updated).

## Summary of Data Flow
`User Input` -> `Session/Env Context` -> `Module/Method/Field Definitions` -> `SM Linking` -> `Orchestrator (Stabilization)` -> `Simulation Output`
