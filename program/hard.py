#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Для своего варианта лабораторной работы 2.17
# необходимо реализовать хранение данных в базе
# данных pgsql. Информация в базе данных
# должна храниться не менее чем в двух таблицах.

import argparse
import typing as t

import psycopg2 as pgsql


def create_db(database_path: str) -> None:
    """
    Создать базу данных.
    """
    try:
        conn = pgsql.connect(
            dbname=database_path, user="aleksejepifanov", host="localhost"
        )
    except  pgsql.OperationalError:
        conn = pgsql.connect(
            dbname="postgres", user="aleksejepifanov", host="localhost"
        )
        cur = conn.cursor()
        conn.autocommit = True
        cur.execute(
            f"CREATE DATABASE {database_path}",
        )
        conn.autocommit = False

        conn.close()
        conn = pgsql.connect(
            dbname=database_path, user="aleksejepifanov", host="localhost"
        )

    cursor = conn.cursor()
    # Создать таблицу с информацией о путях.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS start (
            start_id SERIAL PRIMARY KEY,
            start_point TEXT UNIQUE NOT NULL
        );
        """
    )
    # Создать таблицу с информацией о работниках.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS routes (
            start_id INTEGER NOT NULL,
            route_number INTEGER PRIMARY KEY,
            end_point TEXT NOT NULL,
            FOREIGN KEY(start_id) REFERENCES start(start_id)
        )
        """
    )
    conn.commit()
    conn.close()


def select_all(database_path: str) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать все маршруты.
    """
    conn = conn = pgsql.connect(
        dbname=database_path, user="aleksejepifanov", host="localhost"
    )
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT start.start_point, routes.end_point, routes.route_number
        FROM start
        INNER JOIN routes ON routes.start_id = start.start_id
        """
    )
    rows = cursor.fetchall()

    conn.close()
    return [
        {
            "начальный пункт": row[0],
            "конечный пункт": row[1],
            "номер маршрута": row[2],
        }
        for row in rows
    ]


def add_route(database_path: str, start: str, end: str, count: int) -> None:
    """
    Добавить данные о маршруте.
    """
    start = start.lower()
    end = end.lower()

    conn = pgsql.connect(
        dbname=database_path, user="aleksejepifanov", host="localhost"
    )
    cursor = conn.cursor()

    # Получить идентификатор должности в базе данных.
    # Если такой записи нет, то добавить информацию о новой должности.
    cursor.execute(
        """
        SELECT start_id FROM start WHERE start_point = %s
        """,
        (start,),
    )
    row = cursor.fetchone()
    if not row:
        cursor.execute(
            """
            INSERT INTO start (start_point) VALUES (%s) RETURNING start_id
            """,
            (start,),
        )
        start_id = cursor.fetchone()[0]
    else:
        start_id = row[0]

    # Добавить информацию о новом работнике.
    cursor.execute(
        """
        INSERT INTO routes (start_id, route_number, end_point)
        VALUES (%s, %s, %s)
        """,
        (start_id, count, end),
    )

    conn.commit()
    conn.close()


def display_routes(routes: t.List[t.Dict[str, t.Any]]) -> None:
    """
    Отобразить список маршрутов.
    """
    if routes:
        line = "+-{}-+-{}-+-{}-+".format("-" * 30, "-" * 20, "-" * 8)
        print(line)
        print("| {:^30} | {:^20} | {:^8} |".format("Начало", "Конец", "Номер"))
        print(line)
        for route in routes:
            print(
                "| {:<30} | {:<20} | {:>8} |".format(
                    route.get("начальный пункт", ""),
                    route.get("конечный пункт", ""),
                    route.get("номер маршрута", ""),
                )
            )
        print(line)
    else:
        print("Список маршрутов пуст.")


def select_routes(
    database_path: str, name_point: str
) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать маршруты с заданным пунктом отправления или прибытия.
    """
    conn = pgsql.connect(
        dbname=database_path, user="aleksejepifanov", host="localhost"
    )
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT s.start_point, r.end_point, r.route_number
        FROM start s
        JOIN routes r ON s.start_id = r.start_id
        WHERE s.start_point = %s OR r.end_point = %s
        """,
        (name_point, name_point),
    )
    rows = cursor.fetchall()

    conn.close()
    return [
        {
            "начальный пункт": row[0],
            "конечный пункт": row[1],
            "номер маршрута": row[2],
        }
        for row in rows
    ]


def main(command_line=None):
    """
    Главная функция программы.
    """
    file_parser = argparse.ArgumentParser(add_help=False)
    file_parser.add_argument(
        "--db",
        action="store",
        default="routes",
        help="The database file name",
    )
    parser = argparse.ArgumentParser("routes")
    parser.add_argument(
        "--version", action="version", version="%(prog)s 0.1.0"
    )
    subparsers = parser.add_subparsers(dest="command")
    add = subparsers.add_parser(
        "add", parents=[file_parser], help="Add a new route"
    )
    add.add_argument(
        "-s", "--start", action="store", required=True, help="The route start"
    )
    add.add_argument(
        "-e", "--end", action="store", required=True, help="The route endpoint"
    )
    add.add_argument(
        "-n",
        "--number",
        action="store",
        type=int,
        required=True,
        help="The number of route",
    )

    _ = subparsers.add_parser(
        "list", parents=[file_parser], help="Display all routes"
    )

    select = subparsers.add_parser(
        "select", parents=[file_parser], help="Select the routes"
    )
    select.add_argument(
        "-p",
        "--point",
        action="store",
        required=True,
        help="Routes starting or ending at this point",
    )

    args = parser.parse_args(command_line)

    # Получить путь к файлу базы данных.
    db_path = args.db
    create_db(db_path)

    match args.command:
        case "add":
            add_route(db_path, args.start, args.end, args.number)

        case "list":
            display_routes(select_all(db_path))

        case "select":
            selected = select_routes(db_path, args.point)
            display_routes(selected)


if __name__ == "__main__":
    # main('add -s st -e kt -n 1'.split())
    main()
