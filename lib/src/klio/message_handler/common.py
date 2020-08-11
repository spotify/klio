# Copyright 2020 Spotify AB

import enum

from klio_core.proto import klio_pb2


# [batch dev] maybe add this to the proto definition
# TODO: use this instead of original enum definition from v1.py
class MessageState(enum.Enum):
    PROCESS = 1
    PASS_THRU = 2
    DROP = 3


# TODO: use this instead of original func definition from v1.py
def update_audit_log(dofn_inst, parsed_message, current_job):
    """Update the message's audit log with the current job.

    Args:
        dofn_inst (beam.DoFn): User-implemented DoFn.
        parsed_message (klio_pb2.KlioMessage): a Klio message.
        current_job (klio_pb2.KlioJob): the currently-running job.
    Returns:
        parsed_message (klio_pb2.KlioMessage): a Klio message with
            the audit log updated.
    """
    audit_log_item = klio_pb2.KlioJobAuditLogItem()
    audit_log_item.timestamp.GetCurrentTime()
    audit_log_item.klio_job.CopyFrom(current_job)
    parsed_message.metadata.job_audit_log.extend([audit_log_item])
    audit_log = parsed_message.metadata.job_audit_log
    traversed_dag = " -> ".join(
        "%s::%s" % (str(j.klio_job.gcp_project), str(j.klio_job.job_name))
        for j in audit_log
    )
    traversed_dag = "%s (current job)" % traversed_dag

    base_log_msg = "KlioMessage full audit log"
    log_msg = "%s - Entity ID: %s - Path: %s" % (
        base_log_msg,
        parsed_message.data.entity_id,
        traversed_dag,
    )
    dofn_inst._klio.logger.debug(log_msg)
    return parsed_message
