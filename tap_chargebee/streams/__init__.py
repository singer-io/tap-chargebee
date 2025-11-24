from .addons import AddonsStream
from .coupons import CouponsStream
from .comments import CommentsStream
from .credit_notes import CreditNotesStream
from .customers import CustomersStream
from .events import EventsStream
from .invoices import InvoicesStream
from .item_families import ItemFamiliesStream
from .item_prices import ItemPricesStream
from .items import ItemsStream
from .payment_sources import PaymentSourcesStream
from .plans import PlansStream
from .subscriptions import SubscriptionsStream
from .transactions import TransactionsStream
from .virtual_bank_accounts import VirtualBankAccountsStream
from .credit_notes import CreditNotesStream
from .gifts import GiftsStream
from .orders import OrdersStream
from .quotes import QuotesStream
from .promotional_credits import PromotionalCreditsStream

COMMON_AVAILABLE_STREAMS = [
    EventsStream,
    CommentsStream,
    CouponsStream,
    CreditNotesStream,
    CustomersStream,
    GiftsStream,
    InvoicesStream,
    OrdersStream,
    PaymentSourcesStream,
    QuotesStream,
    PromotionalCreditsStream,
    SubscriptionsStream,
    TransactionsStream,
    VirtualBankAccountsStream
]

PLAN_MODEL_AVAILABLE_STREAMS = COMMON_AVAILABLE_STREAMS + [
    AddonsStream,
    PlansStream
]

ITEM_MODEL_AVAILABLE_STREAMS = COMMON_AVAILABLE_STREAMS + [
    ItemsStream,
    ItemPricesStream,
    ItemFamiliesStream
]

STREAMS = {
    'events' : EventsStream,
    'comments' : CommentsStream,
    'coupons' : CouponsStream,
    'credit_notes' : CreditNotesStream,
    'customers' : CustomersStream,
    'gifts' : GiftsStream,
    'invoices' : InvoicesStream,
    'orders' : OrdersStream,
    'payment_sources' : PaymentSourcesStream,
    'quotes' : QuotesStream,
    'promotional_credits' : PromotionalCreditsStream,
    'subscriptions' : SubscriptionsStream,
    'transactions' : TransactionsStream,
    'virtual_bank_accounts' : VirtualBankAccountsStream,
    'addons' : AddonsStream,
    'plans' : PlansStream,
    'items' : ItemsStream,
    'item_prices' : ItemPricesStream,
    'item_families': ItemFamiliesStream
}