def parse_group_id(task_id):
    """
    Safely parse the group_id and task_name from the task_id.
    If task_id does not contain '.', group_id will be "".
    """
    if "." in task_id:
        group_id, task_name = task_id.rsplit(".", 1)
    else:
        group_id = ""
        task_name = None  # No task_name available

    return group_id, task_name
