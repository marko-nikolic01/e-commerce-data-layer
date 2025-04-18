def generate_email_template(sale_item_anomalies):
    html = """
    <h3>Sale quantity anomaly detected</h3>
    <p>The following customers placed unusually large orders (beyond 2 standard deviations):</p>
    <table border="1" cellpadding="5" cellspacing="0">
        <thead>
            <tr>
                <th>CustomerID</th>
                <th>StockCode</th>
                <th>Quantity</th>
                <th>Deviation</th>
                <th>InvoiceDate</th>
            </tr>
        </thead>
        <tbody>
    """

    rows = sale_item_anomalies.orderBy("InvoiceDate", ascending=False).collect()

    for row in rows:
        html += f"""
        <tr>
            <td>{row['CustomerID']}</td>
            <td>{row['StockCode']}</td>
            <td>{row['Quantity']}</td>
            <td>{row['QuantityDeviation']:.2f}</td>
            <td>{row['InvoiceDate']}</td>
        </tr>
        """

    html += "</tbody></table>"
    return html