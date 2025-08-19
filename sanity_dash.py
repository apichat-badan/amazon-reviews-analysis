from dash import Dash, html
app = Dash(__name__)
app.layout = html.Div("OK")
if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8052, debug=True)
