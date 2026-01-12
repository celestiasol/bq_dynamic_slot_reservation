import pendulum
import google.auth
from google.cloud.bigquery_reservation_v1.services import reservation_service
import google.cloud.bigquery_reservation_v1.types as reservation_types
from google.protobuf import field_mask_pb2
from google.api_core.exceptions import NotFound


def assert_max_slot_value(value):
    if not value:
        raise ValueError('Slot value cannot be null/empty.')
    elif value < 50:
        raise ValueError('Slot value must be at least 50.')
    elif value % 50 != 0:
        raise ValueError('Slot value must be multiple of 50.')
    return value


class BigQuerySlotReservation:
    def __init__(self, **kwargs):
        if not 'project_id' in kwargs:
            _, self.project_id = google.auth.default()
        else:
            self.project_id = kwargs['project_id']
        self.client = reservation_service.ReservationServiceClient(transport='grpc')
        self.reservation_name = self.client.reservation_path(self.project_id, kwargs['zone'], kwargs['reservation_id'])

    def create(self, **kwargs):  # POST
        raise NotImplementedError()

    def get(self):  # GET
        request = reservation_types.reservation.GetReservationRequest(name=self.reservation_name)
        response = self.client.get_reservation(request)

        out = {}
        out['reservation_name'] = response.name
        out['ignore_idle_slots'] = response.ignore_idle_slots
        out['autoscale_current_slots'] = response.autoscale.current_slots
        out['autoscale_max_slots'] = response.autoscale.max_slots
        return out

    def update(self, **kwargs):  # PUT
        # reservation has to be exist first
        """
        reservation_id: str
        slot_capacity: int, default value is 0
        ignore_idle_slots: bool, default value is True
        max_autoscaling_slot: int, default value is 50
        concurrency: int, default value is 0
        """

        try:
            self.client.get_reservation(name=self.reservation_name)
        except NotFound:
            return {
                "status_code": 404,
                "content": f"Reservation {self.reservation_name} not found"
            }

        autoscale = reservation_types.Reservation.Autoscale(max_slots=assert_max_slot_value(kwargs['max_autoscaling_slot']))

        field_mask = field_mask_pb2.FieldMask(paths=["slot_capacity", "ignore_idle_slots", "autoscale", "concurrency"])

        reservation_params = {
            'name': self.reservation_name,
            'slot_capacity': 0,
            'ignore_idle_slots': kwargs['ignore_idle_slots'],
            'autoscale': autoscale,
            'concurrency': 0
        }

        reservation = reservation_types.Reservation(**reservation_params)

        request = reservation_types.reservation.UpdateReservationRequest(reservation=reservation, update_mask=field_mask)

        response = self.client.update_reservation(request=request)

        updated_time_utc7 = str(response.update_time.astimezone(pendulum.timezone('Asia/Jakarta')))

        return {
            "status_code": 200,
            "content": f"Reservation has been update at {updated_time_utc7}"
        }

    def delete(self, **kwargs):
        raise NotImplementedError("Reservation deletion is prohibited.")

