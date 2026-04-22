def create_pattern_spec_prompt(
        goal:str = f"""
        
        """,
        errors:list[str] = [],
        cases_ds:dict[str, dict[str, dict]]={},

):
    component_ids = list(cases_ds.keys())
    return f"""
TASK DESCRIPTION:
Based on your goal, 

GOAL:
{goal}

CASES:
{cases_ds}


Be aware of the followign errors:
{prev_results}


Return the data structure in format 

"""