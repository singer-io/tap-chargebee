from .addons import AddonsStream
from .coupons import CouponsStream
from .credit_notes import CreditNotesStream
from .customers import CustomersStream
from .events import EventsStream
from .invoices import InvoicesStream
from .payment_sources import PaymentSourcesStream
from .plans import PlansStream
from .subscriptions import SubscriptionsStream
from .transactions import TransactionsStream
from .virtual_bank_accounts import VirtualBankAccountsStream
from .credit_notes import CreditNotesStream
from .gifts import GiftsStream
from .orders import OrdersStream
from.promotional_credits import PromotionalCreditsStream

AVAILABLE_STREAMS = [
    EventsStream,
    AddonsStream,
    CouponsStream,
    CreditNotesStream,
    CustomersStream,
    GiftsStream,
    InvoicesStream,
    OrdersStream,
    PaymentSourcesStream,
    PromotionalCreditsStream,
    PlansStream,
    SubscriptionsStream,
    TransactionsStream,
    VirtualBankAccountsStream
]
