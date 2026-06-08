from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html, dash_table

PROJECT_HOME = Path(os.getenv("PROJECT_HOME", Path(__file__).resolve().parents[1]))
DWH_DB_PATH = Path(os.getenv("DWH_DB_PATH", PROJECT_HOME / "data/runtime/dwh.duckdb"))
DASHBOARD_HOST = os.getenv("DASHBOARD_HOST", "0.0.0.0")
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "8050"))

PAGE_STYLE = {
    "fontFamily": "Arial, sans-serif",
    "backgroundColor": "#f6f7fb",
    "minHeight": "100vh",
    "padding": "24px",
    "color": "#1f2937",
}
CARD_STYLE = {
    "backgroundColor": "white",
    "borderRadius": "16px",
    "padding": "18px",
    "boxShadow": "0 8px 24px rgba(15, 23, 42, 0.08)",
}
KPI_CARD_STYLE = {
    **CARD_STYLE,
    "minHeight": "108px",
    "display": "flex",
    "flexDirection": "column",
    "justifyContent": "space-between",
}
GRID_STYLE = {
    "display": "grid",
    "gridTemplateColumns": "repeat(auto-fit, minmax(220px, 1fr))",
    "gap": "16px",
    "marginTop": "18px",
}
CHART_GRID_STYLE = {
    "display": "grid",
    "gridTemplateColumns": "repeat(auto-fit, minmax(520px, 1fr))",
    "gap": "18px",
    "marginTop": "18px",
}


def empty_figure(title: str, message: str = "Нет данных для выбранного периода") -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        title=title,
        template="plotly_white",
        height=360,
        annotations=[
            {
                "text": message,
                "xref": "paper",
                "yref": "paper",
                "x": 0.5,
                "y": 0.5,
                "showarrow": False,
                "font": {"size": 16},
            }
        ],
    )
    return fig


def fmt_money(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{value:,.0f} ₽".replace(",", " ")


def fmt_number(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{value:,.0f}".replace(",", " ")


def fmt_percent(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{value:.1f}%"


def query_df(sql: str) -> pd.DataFrame:
    with duckdb.connect(str(DWH_DB_PATH), read_only=True) as con:
        return con.execute(sql).fetchdf()


def load_dashboard_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str | None]:
    if not DWH_DB_PATH.exists():
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), (
            f"DuckDB файл ещё не создан: {DWH_DB_PATH}. Запусти DAG-и в Airflow или локальный dbt pipeline."
        )

    try:
        daily_df = query_df(
            """
            SELECT
                revenue_date,
                checks_count,
                guests_count,
                fact_total_amount,
                plan_total_amount,
                plan_fact_delta_amount,
                plan_completion_rate,
                fact_restaurant_amount,
                plan_restaurant_amount,
                fact_delivery_amount,
                plan_delivery_and_aggregator_amount,
                fact_pickup_amount,
                plan_pickup_amount,
                fact_banquet_amount,
                plan_banquet_amount,
                fact_other_amount,
                avg_check_per_order,
                avg_check_per_guest,
                prev_year_fact_total_amount,
                prev_year_checks_count,
                prev_year_avg_check_per_order,
                yoy_growth_rate
            FROM mart.mart_dashboard_daily
            ORDER BY revenue_date
            """
        )
        channel_df = query_df(
            """
            SELECT
                revenue_date,
                channel_group,
                checks_count,
                guests_count,
                revenue_amount,
                avg_check_per_order,
                avg_revenue_per_guest
            FROM mart.mart_channel_daily
            ORDER BY revenue_date, channel_group
            """
        )
        dish_df = query_df(
            """
            SELECT
                revenue_date,
                category_clean,
                dish_name,
                qty_sold,
                dish_revenue_amount,
                checks_count
            FROM mart.mart_dish_sales_daily
            ORDER BY revenue_date, dish_revenue_amount DESC
            """
        )
    except Exception as exc:  # noqa: BLE001 - dashboard should show a friendly error.
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), (
            "Не удалось прочитать mart-таблицы. Проверь, что dbt run уже отработал. "
            f"Техническая ошибка: {exc}"
        )

    for df in (daily_df, channel_df, dish_df):
        if "revenue_date" in df.columns:
            df["revenue_date"] = pd.to_datetime(df["revenue_date"]).dt.date

    return daily_df, channel_df, dish_df, None


def default_dates(daily_df: pd.DataFrame) -> tuple[str | None, str | None, str | None, str | None]:
    if daily_df.empty:
        return None, None, None, None

    min_date = daily_df["revenue_date"].min()
    max_date = daily_df["revenue_date"].max()
    default_start = max_date.replace(day=1)
    return (
        min_date.isoformat(),
        max_date.isoformat(),
        default_start.isoformat(),
        max_date.isoformat(),
    )


def filter_by_date(df: pd.DataFrame, start_date: str | None, end_date: str | None) -> pd.DataFrame:
    if df.empty or "revenue_date" not in df.columns:
        return df

    filtered = df.copy()
    if start_date:
        start = pd.to_datetime(start_date).date()
        filtered = filtered[filtered["revenue_date"] >= start]
    if end_date:
        end = pd.to_datetime(end_date).date()
        filtered = filtered[filtered["revenue_date"] <= end]
    return filtered


def kpi_card(title: str, value: str, subtitle: str | None = None) -> html.Div:
    return html.Div(
        [
            html.Div(title, style={"fontSize": "13px", "color": "#6b7280", "fontWeight": 600}),
            html.Div(value, style={"fontSize": "30px", "fontWeight": 800, "marginTop": "8px"}),
            html.Div(subtitle or "", style={"fontSize": "13px", "color": "#6b7280", "marginTop": "8px"}),
        ],
        style=KPI_CARD_STYLE,
    )


def build_kpis(df: pd.DataFrame) -> html.Div:
    if df.empty:
        return html.Div([kpi_card("Нет данных", "—", "Выбери другой период или запусти pipeline")], style=GRID_STYLE)

    fact = df["fact_total_amount"].fillna(0).sum()
    plan = df["plan_total_amount"].fillna(0).sum()
    prev_year = df["prev_year_fact_total_amount"].fillna(0).sum()
    checks = df["checks_count"].fillna(0).sum()
    guests = df["guests_count"].fillna(0).sum()

    completion = fact / plan * 100 if plan else None
    yoy_growth = (fact / prev_year - 1) * 100 if prev_year else None
    avg_check = fact / checks if checks else None
    avg_guest = fact / guests if guests else None
    delta = fact - plan if plan else None

    cards = [
        kpi_card("Факт выручка", fmt_money(fact), f"Чеков: {fmt_number(checks)}"),
        kpi_card("План", fmt_money(plan), f"Отклонение: {fmt_money(delta)}" if delta is not None else "План не задан"),
        kpi_card("Выполнение плана", fmt_percent(completion), "Факт / план"),
        kpi_card("Прирост к прошлому году", fmt_percent(yoy_growth), f"LY: {fmt_money(prev_year)}" if prev_year else "Нет базы LY"),
        kpi_card("Средний чек", fmt_money(avg_check), "Факт / кол-во чеков"),
        kpi_card("Выручка на гостя", fmt_money(avg_guest), f"Гостей: {fmt_number(guests)}"),
    ]
    return html.Div(cards, style=GRID_STYLE)


def plan_fact_figure(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return empty_figure("План / факт по дням")
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=df["revenue_date"],
            y=df["fact_total_amount"],
            name="Факт",
            hovertemplate="%{x}<br>Факт: %{y:,.0f} ₽<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df["revenue_date"],
            y=df["plan_total_amount"],
            name="План",
            mode="lines+markers",
            hovertemplate="%{x}<br>План: %{y:,.0f} ₽<extra></extra>",
        )
    )
    fig.update_layout(
        title="План / факт по дням",
        template="plotly_white",
        height=380,
        margin={"l": 40, "r": 20, "t": 60, "b": 40},
        yaxis_title="₽",
        legend_orientation="h",
    )
    return fig


def yoy_figure(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return empty_figure("Факт vs прошлый год")
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=df["revenue_date"],
            y=df["fact_total_amount"],
            name="Факт текущего года",
            hovertemplate="%{x}<br>Факт: %{y:,.0f} ₽<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df["revenue_date"],
            y=df["prev_year_fact_total_amount"],
            name="Факт LY",
            mode="lines+markers",
            hovertemplate="%{x}<br>LY: %{y:,.0f} ₽<extra></extra>",
        )
    )
    fig.update_layout(
        title="Факт vs прошлый год по тем же датам",
        template="plotly_white",
        height=380,
        margin={"l": 40, "r": 20, "t": 60, "b": 40},
        yaxis_title="₽",
        legend_orientation="h",
    )
    return fig


def channel_figure(channel_df: pd.DataFrame) -> go.Figure:
    if channel_df.empty:
        return empty_figure("Выручка по каналам")
    grouped = (
        channel_df.groupby("channel_group", as_index=False)
        .agg(revenue_amount=("revenue_amount", "sum"), checks_count=("checks_count", "sum"))
        .sort_values("revenue_amount", ascending=False)
    )
    fig = go.Figure(
        go.Bar(
            x=grouped["channel_group"],
            y=grouped["revenue_amount"],
            customdata=grouped[["checks_count"]],
            hovertemplate="Канал: %{x}<br>Выручка: %{y:,.0f} ₽<br>Чеки: %{customdata[0]:,.0f}<extra></extra>",
        )
    )
    fig.update_layout(
        title="Выручка по каналам",
        template="plotly_white",
        height=380,
        margin={"l": 40, "r": 20, "t": 60, "b": 40},
        yaxis_title="₽",
    )
    return fig


def avg_check_figure(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return empty_figure("Средний чек")
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df["revenue_date"],
            y=df["avg_check_per_order"],
            name="Средний чек",
            mode="lines+markers",
            hovertemplate="%{x}<br>Средний чек: %{y:,.0f} ₽<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df["revenue_date"],
            y=df["avg_check_per_guest"],
            name="На гостя",
            mode="lines+markers",
            hovertemplate="%{x}<br>На гостя: %{y:,.0f} ₽<extra></extra>",
        )
    )
    fig.update_layout(
        title="Средний чек и выручка на гостя",
        template="plotly_white",
        height=380,
        margin={"l": 40, "r": 20, "t": 60, "b": 40},
        yaxis_title="₽",
        legend_orientation="h",
    )
    return fig


def top_dishes_figure(dish_df: pd.DataFrame) -> go.Figure:
    if dish_df.empty:
        return empty_figure("Топ блюд по выручке")
    grouped = (
        dish_df.groupby(["category_clean", "dish_name"], as_index=False)
        .agg(dish_revenue_amount=("dish_revenue_amount", "sum"), qty_sold=("qty_sold", "sum"))
        .sort_values("dish_revenue_amount", ascending=False)
        .head(15)
        .sort_values("dish_revenue_amount", ascending=True)
    )
    labels = grouped["dish_name"] + " / " + grouped["category_clean"].fillna("без категории")
    fig = go.Figure(
        go.Bar(
            x=grouped["dish_revenue_amount"],
            y=labels,
            orientation="h",
            customdata=grouped[["qty_sold"]],
            hovertemplate="%{y}<br>Выручка: %{x:,.0f} ₽<br>Кол-во: %{customdata[0]:,.1f}<extra></extra>",
        )
    )
    fig.update_layout(
        title="Топ-15 блюд по выручке",
        template="plotly_white",
        height=520,
        margin={"l": 220, "r": 20, "t": 60, "b": 40},
        xaxis_title="₽",
    )
    return fig


def detail_table(df: pd.DataFrame) -> dash_table.DataTable:
    table_df = df.copy()
    if table_df.empty:
        table_df = pd.DataFrame(
            columns=["Дата", "Факт", "План", "Выполнение", "YoY", "Чеки", "Средний чек"]
        )
    else:
        table_df = table_df[[
            "revenue_date",
            "fact_total_amount",
            "plan_total_amount",
            "plan_completion_rate",
            "yoy_growth_rate",
            "checks_count",
            "avg_check_per_order",
        ]].copy()
        table_df["Дата"] = table_df["revenue_date"].astype(str)
        table_df["Факт"] = table_df["fact_total_amount"].map(fmt_money)
        table_df["План"] = table_df["plan_total_amount"].map(fmt_money)
        table_df["Выполнение"] = (table_df["plan_completion_rate"] * 100).map(fmt_percent)
        table_df["YoY"] = (table_df["yoy_growth_rate"] * 100).map(fmt_percent)
        table_df["Чеки"] = table_df["checks_count"].map(fmt_number)
        table_df["Средний чек"] = table_df["avg_check_per_order"].map(fmt_money)
        table_df = table_df[["Дата", "Факт", "План", "Выполнение", "YoY", "Чеки", "Средний чек"]]

    return dash_table.DataTable(
        data=table_df.to_dict("records"),
        columns=[{"name": col, "id": col} for col in table_df.columns],
        page_size=12,
        style_table={"overflowX": "auto"},
        style_cell={"fontFamily": "Arial", "fontSize": 13, "padding": "8px", "textAlign": "left"},
        style_header={"fontWeight": "bold", "backgroundColor": "#f3f4f6"},
    )


def serve_layout() -> html.Div:
    daily_df, _, _, error = load_dashboard_data()
    min_date, max_date, start_date, end_date = default_dates(daily_df)

    error_block: list[Any] = []
    if error:
        error_block = [
            html.Div(
                error,
                style={
                    **CARD_STYLE,
                    "borderLeft": "5px solid #ef4444",
                    "color": "#991b1b",
                    "marginTop": "18px",
                },
            )
        ]

    return html.Div(
        [
            html.Div(
                [
                    html.Div(
                        [
                            html.H1("Italy DWH Dashboard", style={"margin": 0, "fontSize": "34px"}),
                            html.Div(
                                "План/факт, YoY-прирост, средний чек, каналы и топ блюд",
                                style={"color": "#6b7280", "marginTop": "6px"},
                            ),
                        ]
                    ),
                    html.Div(
                        [
                            html.Div("Период", style={"fontWeight": 700, "marginBottom": "8px"}),
                            dcc.DatePickerRange(
                                id="date-range",
                                min_date_allowed=min_date,
                                max_date_allowed=max_date,
                                start_date=start_date,
                                end_date=end_date,
                                display_format="DD.MM.YYYY",
                                first_day_of_week=1,
                            ),
                            html.Button(
                                "Обновить",
                                id="refresh-button",
                                n_clicks=0,
                                style={
                                    "marginLeft": "12px",
                                    "padding": "9px 14px",
                                    "borderRadius": "10px",
                                    "border": "1px solid #d1d5db",
                                    "backgroundColor": "white",
                                    "cursor": "pointer",
                                },
                            ),
                        ],
                        style={**CARD_STYLE, "minWidth": "420px"},
                    ),
                ],
                style={
                    "display": "flex",
                    "justifyContent": "space-between",
                    "alignItems": "center",
                    "gap": "18px",
                    "flexWrap": "wrap",
                },
            ),
            *error_block,
            html.Div(id="dashboard-content"),
        ],
        style=PAGE_STYLE,
    )


app = Dash(__name__, title="Italy DWH Dashboard", suppress_callback_exceptions=True)
server = app.server
app.layout = serve_layout


@app.callback(
    Output("dashboard-content", "children"),
    Input("date-range", "start_date"),
    Input("date-range", "end_date"),
    Input("refresh-button", "n_clicks"),
)
def update_dashboard(start_date: str | None, end_date: str | None, _: int) -> list[Any]:
    daily_df, channel_df, dish_df, error = load_dashboard_data()
    if error:
        return []

    filtered_daily = filter_by_date(daily_df, start_date, end_date)
    filtered_channel = filter_by_date(channel_df, start_date, end_date)
    filtered_dish = filter_by_date(dish_df, start_date, end_date)

    return [
        build_kpis(filtered_daily),
        html.Div(
            [
                html.Div(dcc.Graph(figure=plan_fact_figure(filtered_daily)), style=CARD_STYLE),
                html.Div(dcc.Graph(figure=yoy_figure(filtered_daily)), style=CARD_STYLE),
                html.Div(dcc.Graph(figure=channel_figure(filtered_channel)), style=CARD_STYLE),
                html.Div(dcc.Graph(figure=avg_check_figure(filtered_daily)), style=CARD_STYLE),
            ],
            style=CHART_GRID_STYLE,
        ),
        html.Div(
            [
                html.Div(dcc.Graph(figure=top_dishes_figure(filtered_dish)), style=CARD_STYLE),
                html.Div(
                    [html.H3("Детализация по дням", style={"marginTop": 0}), detail_table(filtered_daily)],
                    style=CARD_STYLE,
                ),
            ],
            style={"display": "grid", "gridTemplateColumns": "minmax(520px, 1fr)", "gap": "18px", "marginTop": "18px"},
        ),
    ]


if __name__ == "__main__":
    app.run(host=DASHBOARD_HOST, port=DASHBOARD_PORT, debug=False)
