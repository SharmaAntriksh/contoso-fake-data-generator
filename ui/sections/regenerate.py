import streamlit as st

FORCE_REGENERATABLE_DIMENSIONS = [
    "customers",
    "products",
    "stores",
    "promotions",
    "dates",
    "currency",
    "exchange_rates",
]


def render_regeneration():
    st.subheader("🔄 Regenerate dimensions")

    if "force_regenerate_dimensions" not in st.session_state:
        st.session_state.force_regenerate_dimensions = set()

    # ---- Global option ----
    regen_all = st.checkbox(
        "Regenerate all dimensions",
        key="regen_all_dimensions",
        help="Forces regeneration of all dimensions, ignoring cache.",
    )

    # Compact separator (much tighter than st.divider)
    st.markdown("<hr style='margin: 0.75rem 0;'>", unsafe_allow_html=True)

    selected = set()

    if regen_all:
        selected = {
            "customers",
            "products",
            "stores",
            "promotions",
            "dates",
            "currency",
            "exchange_rates",
        }
    else:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Core entities**")
            if st.checkbox("Customers", key="regen_customers"):
                selected.add("customers")
            if st.checkbox("Stores", key="regen_stores"):
                selected.add("stores")
            if st.checkbox("Products", key="regen_products"):
                selected.add("products")

        with col2:
            st.markdown("**Time & financials**")
            if st.checkbox("Promotions", key="regen_promotions"):
                selected.add("promotions")
            if st.checkbox("Dates", key="regen_dates"):
                selected.add("dates")
            if st.checkbox("Currency", key="regen_currency"):
                selected.add("currency")
            if st.checkbox("Exchange rates", key="regen_exchange_rates"):
                selected.add("exchange_rates")

    st.session_state.force_regenerate_dimensions = selected
