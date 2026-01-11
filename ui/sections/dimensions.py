import streamlit as st

# =========================================================
# Dimension size controls (config-driven)
# =========================================================

DIMENSION_SIZE_FIELDS = {
    "customers": "total_customers",
    "products": "num_products",
    "stores": "num_stores",
    "promotions": "num_seasonal",
}

# Dimensions that support force regeneration from UI
FORCE_REGENERATABLE_DIMENSIONS = [
    "customers",
    "products",
    "stores",
    "promotions",
    "dates",
    "currency",
    "exchange_rates",
]


def render_dimensions(cfg):
    st.subheader("4️⃣ Dimensions")

    def dim(section, label, step, min_val=1):
        field = DIMENSION_SIZE_FIELDS[section]
        cfg[section][field] = st.number_input(
            label,
            min_value=min_val,
            step=step,
            value=cfg[section][field],
        )

    col1, col2 = st.columns(2)

    with col1:
        dim("customers", "Customers (entities)", step=1_000)
        dim("stores", "Physical stores", step=10)

    with col2:
        dim("products", "Products (SKUs)", step=500)
        dim("promotions", "Active promotions", step=5, min_val=0)


    # -----------------------------------------------------
    # NOTE:
    # Pricing (product behavior) is rendered AFTER this
    # function by the caller. Regeneration controls must
    # therefore stay LAST in this section.
    # -----------------------------------------------------