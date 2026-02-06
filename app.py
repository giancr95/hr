import sqlite3
from contextlib import closing
from datetime import date

import pandas as pd
import streamlit as st

DB_PATH = "employees.db"


def get_connection():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def init_db():
    with closing(get_connection()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS employees (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                full_name TEXT NOT NULL,
                role_title TEXT NOT NULL,
                hourly_rate REAL NOT NULL,
                start_date TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS work_hours (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                employee_id INTEGER NOT NULL,
                work_date TEXT NOT NULL,
                hours REAL NOT NULL,
                notes TEXT,
                FOREIGN KEY (employee_id) REFERENCES employees (id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS adjustments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                employee_id INTEGER NOT NULL,
                adjustment_date TEXT NOT NULL,
                adjustment_type TEXT NOT NULL,
                amount REAL NOT NULL,
                description TEXT,
                FOREIGN KEY (employee_id) REFERENCES employees (id)
            )
            """
        )
        conn.commit()


def add_employee(full_name, role_title, hourly_rate, start_date):
    with closing(get_connection()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO employees (full_name, role_title, hourly_rate, start_date)
            VALUES (?, ?, ?, ?)
            """,
            (full_name, role_title, hourly_rate, start_date.isoformat()),
        )
        conn.commit()


def add_work_hours(employee_id, work_date, hours, notes):
    with closing(get_connection()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO work_hours (employee_id, work_date, hours, notes)
            VALUES (?, ?, ?, ?)
            """,
            (employee_id, work_date.isoformat(), hours, notes),
        )
        conn.commit()


def add_adjustment(employee_id, adjustment_date, adjustment_type, amount, description):
    with closing(get_connection()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO adjustments (
                employee_id, adjustment_date, adjustment_type, amount, description
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                employee_id,
                adjustment_date.isoformat(),
                adjustment_type,
                amount,
                description,
            ),
        )
        conn.commit()


def load_employees():
    with closing(get_connection()) as conn:
        return pd.read_sql_query(
            "SELECT id, full_name, role_title, hourly_rate, start_date FROM employees",
            conn,
        )


def load_work_hours(start_date, end_date):
    with closing(get_connection()) as conn:
        return pd.read_sql_query(
            """
            SELECT work_hours.id, employees.full_name, work_hours.employee_id,
                   work_hours.work_date, work_hours.hours, work_hours.notes
            FROM work_hours
            JOIN employees ON employees.id = work_hours.employee_id
            WHERE work_hours.work_date BETWEEN ? AND ?
            ORDER BY work_hours.work_date
            """,
            conn,
            params=(start_date.isoformat(), end_date.isoformat()),
        )


def load_adjustments(start_date, end_date):
    with closing(get_connection()) as conn:
        return pd.read_sql_query(
            """
            SELECT adjustments.id, employees.full_name, adjustments.employee_id,
                   adjustments.adjustment_date, adjustments.adjustment_type,
                   adjustments.amount, adjustments.description
            FROM adjustments
            JOIN employees ON employees.id = adjustments.employee_id
            WHERE adjustments.adjustment_date BETWEEN ? AND ?
            ORDER BY adjustments.adjustment_date
            """,
            conn,
            params=(start_date.isoformat(), end_date.isoformat()),
        )


def calculate_payroll(start_date, end_date):
    with closing(get_connection()) as conn:
        employees = pd.read_sql_query(
            "SELECT id, full_name, hourly_rate FROM employees", conn
        )
        if employees.empty:
            return pd.DataFrame()

        hours = pd.read_sql_query(
            """
            SELECT employee_id, SUM(hours) AS total_hours
            FROM work_hours
            WHERE work_date BETWEEN ? AND ?
            GROUP BY employee_id
            """,
            conn,
            params=(start_date.isoformat(), end_date.isoformat()),
        )

        bonuses = pd.read_sql_query(
            """
            SELECT employee_id, SUM(amount) AS bonus_total
            FROM adjustments
            WHERE adjustment_date BETWEEN ? AND ?
              AND adjustment_type = 'bonus'
            GROUP BY employee_id
            """,
            conn,
            params=(start_date.isoformat(), end_date.isoformat()),
        )

        deductions = pd.read_sql_query(
            """
            SELECT employee_id, SUM(amount) AS deduction_total
            FROM adjustments
            WHERE adjustment_date BETWEEN ? AND ?
              AND adjustment_type = 'deduction'
            GROUP BY employee_id
            """,
            conn,
            params=(start_date.isoformat(), end_date.isoformat()),
        )

    payroll = employees.merge(hours, how="left", left_on="id", right_on="employee_id")
    payroll = payroll.merge(bonuses, how="left", on="employee_id")
    payroll = payroll.merge(deductions, how="left", on="employee_id")

    payroll["total_hours"] = payroll["total_hours"].fillna(0)
    payroll["bonus_total"] = payroll["bonus_total"].fillna(0)
    payroll["deduction_total"] = payroll["deduction_total"].fillna(0)
    payroll["gross_pay"] = payroll["total_hours"] * payroll["hourly_rate"]
    payroll["net_pay"] = (
        payroll["gross_pay"] + payroll["bonus_total"] - payroll["deduction_total"]
    )

    return payroll[
        [
            "full_name",
            "hourly_rate",
            "total_hours",
            "gross_pay",
            "bonus_total",
            "deduction_total",
            "net_pay",
        ]
    ].sort_values("full_name")


def render_employee_section(employees):
    st.subheader("Registrar empleado")
    with st.form("employee_form", clear_on_submit=True):
        full_name = st.text_input("Nombre completo")
        role_title = st.text_input("Puesto")
        hourly_rate = st.number_input("Tarifa por hora", min_value=0.0, step=1.0)
        start_date = st.date_input("Fecha de ingreso", value=date.today())
        submitted = st.form_submit_button("Guardar empleado")
        if submitted:
            if not full_name or not role_title:
                st.error("Completa el nombre y el puesto para continuar.")
            else:
                add_employee(full_name, role_title, hourly_rate, start_date)
                st.success("Empleado registrado.")

    if not employees.empty:
        st.dataframe(employees, use_container_width=True)


def render_hours_section(employees):
    st.subheader("Registrar horas trabajadas")
    if employees.empty:
        st.info("Primero registra al menos un empleado.")
        return

    with st.form("hours_form", clear_on_submit=True):
        employee_label = st.selectbox(
            "Empleado", employees["full_name"], key="hours_employee"
        )
        work_date = st.date_input("Fecha trabajada", value=date.today())
        hours = st.number_input("Horas", min_value=0.0, step=0.5)
        notes = st.text_input("Notas")
        submitted = st.form_submit_button("Guardar horas")
        if submitted:
            employee_id = int(
                employees.loc[employees["full_name"] == employee_label, "id"].iloc[0]
            )
            add_work_hours(employee_id, work_date, hours, notes)
            st.success("Horas registradas.")


def render_adjustments_section(employees):
    st.subheader("Registrar bonificaciones o deducciones")
    if employees.empty:
        st.info("Primero registra al menos un empleado.")
        return

    with st.form("adjustments_form", clear_on_submit=True):
        employee_label = st.selectbox(
            "Empleado", employees["full_name"], key="adjust_employee"
        )
        adjustment_date = st.date_input("Fecha del movimiento", value=date.today())
        adjustment_type = st.selectbox("Tipo", ["bonus", "deduction"])
        amount = st.number_input("Monto", min_value=0.0, step=1.0)
        description = st.text_input("Descripción")
        submitted = st.form_submit_button("Guardar movimiento")
        if submitted:
            employee_id = int(
                employees.loc[employees["full_name"] == employee_label, "id"].iloc[0]
            )
            add_adjustment(
                employee_id, adjustment_date, adjustment_type, amount, description
            )
            st.success("Movimiento registrado.")


def render_payroll_section():
    st.subheader("Resumen de quincena")
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Fecha inicio", value=date.today().replace(day=1), key="payroll_start"
        )
    with col2:
        end_date = st.date_input(
            "Fecha fin", value=date.today(), key="payroll_end"
        )

    if start_date > end_date:
        st.error("La fecha de inicio no puede ser mayor que la fecha fin.")
        return

    payroll = calculate_payroll(start_date, end_date)
    if payroll.empty:
        st.info("No hay movimientos para el rango seleccionado.")
        return

    st.dataframe(payroll, use_container_width=True)
    st.metric("Total a depositar", f"$ {payroll['net_pay'].sum():,.2f}")

    hours = load_work_hours(start_date, end_date)
    adjustments = load_adjustments(start_date, end_date)

    with st.expander("Detalle de horas"):
        if hours.empty:
            st.write("Sin horas registradas.")
        else:
            st.dataframe(hours, use_container_width=True)

    with st.expander("Detalle de movimientos"):
        if adjustments.empty:
            st.write("Sin movimientos registrados.")
        else:
            st.dataframe(adjustments, use_container_width=True)


def main():
    st.set_page_config(page_title="Gestor de nómina", layout="wide")
    st.title("Gestor de empleados y nómina")
    st.write(
        "Administra empleados, registra horas, bonificaciones y deducciones, y calcula la nómina por quincena."
    )

    init_db()
    employees = load_employees()

    render_employee_section(employees)
    st.divider()
    render_hours_section(employees)
    st.divider()
    render_adjustments_section(employees)
    st.divider()
    render_payroll_section()


if __name__ == "__main__":
    main()
