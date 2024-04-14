#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Для своего варианта лабораторной работы 2.17 
# необходимо реализовать хранение данных в базе 
# данных SQLite3. Информация в базе данных 
# должна храниться не менее чем в двух таблицах.

import argparse
import sqlite3
import typing as t
from pathlib import Path


def create_db(database_path: Path) -> None:
    """
    Создать базу данных.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    # Создать таблицу с информацией о путях.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS start (
            start_id INTEGER PRIMARY KEY AUTOINCREMENT,
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

    conn.close()


def select_all(database_path: Path) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать все маршруты.
    """
    conn = sqlite3.connect(database_path)
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


def add_route(database_path: Path, start: str, end: str, count: int) -> None:
    """
    Добавить данные о маршруте.
    """
    start = start.lower()
    end = end.lower()

    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    # Получить идентификатор должности в базе данных.
    # Если такой записи нет, то добавить информацию о новой должности.
    cursor.execute(
        """
        SELECT start_id FROM start WHERE start_point = ?
        """,
        (start,),
    )
    row = cursor.fetchone()
    if not row:
        cursor.execute(
            """
            INSERT INTO start (start_point) VALUES (?)
            """,
            (start,),
        )
        start_id = cursor.lastrowid
    else:
        start_id = row[0]

    # Добавить информацию о новом работнике.
    cursor.execute(
        """
        INSERT INTO routes (start_id, route_number, end_point)
        VALUES (?, ?, ?)
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
    database_path: Path, name_point: str
) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать маршруты с заданным пунктом отправления или прибытия.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT s.start_point, r.end_point, r.route_number
        FROM start s
        JOIN routes r ON s.start_id = r.start_id
        WHERE s.start_point = ? OR r.end_point = ?
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
        default=str(Path.home() / "routes.db"),
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
    db_path = Path(args.db)
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
    main()
