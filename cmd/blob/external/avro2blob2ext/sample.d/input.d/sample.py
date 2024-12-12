from typing import Iterator
from typing import Tuple
from typing import Callable

import sqlite3

printString: Callable[[str], Callable[[], None]] = lambda s: lambda: print(s)

def filename2iter2sqlite(filename: str)->Callable[[Iterator[dict]], Callable[[], None]]:
	def iter2sqlite(rows: Iterator[dict])->Callable[[], None]:
		def ret()->None:
			with sqlite3.connect(filename) as con:
				con.execute('''
					DROP TABLE IF EXISTS sample_table1
				''')

				con.execute('''
					CREATE TABLE IF NOT EXISTS sample_table1(
						pid INTEGER PRIMARY KEY,
						price REAL NOT NULL,
						name TEXT NOT NULL,
						data BLOB NOT NULL
					)
				''')

				con.executemany(
					'''
						INSERT INTO sample_table1
						VALUES(
						  ?,
						  ?,
						  ?,
						  ?
						)
					''',
					map(lambda d: (
						d["pid"],
						d["price"],
						d["name"],
						d["data"],
					), rows),
				)
				pass
			pass
		return ret
	return iter2sqlite

iter2sqlite: Callable[[Iterator[dict]], Callable[[], None]] = filename2iter2sqlite(
	"./sample.sqlite.db",
)

iter2sqlite(iter([
	dict(
		pid=42,
		price=42.195,
		name="run",
		data=b"helo",
	),
	dict(
		pid=634,
		price=3.776,
		name="fuji",
		data=b"mount",
	),
]))()
