from rest_framework.exceptions import APIException


class BadGateway(APIException):
    """A Kafka publish failed."""

    status_code = 502
    default_detail = "Failed to publish to Kafka."
    default_code = "bad_gateway"


class Conflict(APIException):
    # TODO - Cumbersome - remove
    """A uniqueness constraint was violated."""

    status_code = 409
    default_detail = "Already exists."
    default_code = "conflict"
