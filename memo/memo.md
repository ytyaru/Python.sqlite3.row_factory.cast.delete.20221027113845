delete文をラップした【Python】

　表名と行を特定する列とその値を指定することでdelete文を作る。

<!-- more -->

# ブツ

* [リポジトリ][]

[リポジトリ]:https://github.com/ytyaru/Python.sqlite3.row_factory.cast.delete.20221027113845
[DEMO]:https://ytyaru.github.io/Python.sqlite3.row_factory.cast.delete.20221027113845/

## 実行

```sh
NAME='Python.sqlite3.row_factory.cast.delete.20221027113845'
git clone https://github.com/ytyaru/$NAME
cd $NAME/src
./run.py
./test.py
```

# コード抜粋

```python
class DeleteSqlBuilder(SqlBuilder):
    def build(self):
        table_name = self.row.__class__.__name__
        where = 'where ' + ' and '.join(self._get_preperds())
        return f"delete from {table_name} {where};", tuple(self._get_vals())
```

　前回の`update`文と共通の部分があるので抽象クラスにした。

```python
class SqlBuilder(metaclass=ABCMeta):
    def __init__(self, row, *args, **kwargs): self.row = row
    def _get_target_cols_kv(self, target=None): return (target or self.row)._asdict().items()
    def _get_vals(self, target=None): return [getattr((target or self.row), k) for k,v in self._get_target_cols_kv(target) if v != NOT_USE]
    def _get_preperds(self, target=None): return [f'{k}=?' for k,v in self._get_target_cols_kv(target) if v != NOT_USE]
    #def _get_vals(self, target=None): return [getattr((target or self.row), k) for k,v in self._get_target_cols_kv(target) if v != NOT_USE]
    #def _get_preperds(self, target=None): return [f'{k}=?' for k,v in self._get_target_cols_kv(target) if v != NOT_USE]
    @abstractmethod
    def build(self): pass
```

　前回のupdate文は以下。

```python
class UpdateSqlBuilder(SqlBuilder):
    def __init__(self, row, where=None):
        super().__init__(row)
        self.where = where
    def build(self): #self.row,whereはget_self.row()の戻り値で得た型のインスタンスであること
        if isinstance(self.row, type): raise ValueError('引数rowは型でなくインスタンスを指定してください。')
        table_name = self.row.__class__.__name__
        set_sql = self._set_sql()
        if 0 == len(set_sql): raise ValueError('引数rowに更新するデータをセットしてください。')
        return f"update {table_name} set {set_sql} {self._where_sql()};", tuple(self._get_vals() + (self._get_vals(self.where) if self.where else [self.row.id]))

    def _get_target_cols_kv(self, target=None):
        #d = super()._get_target_cols_kv()
        d = (target or self.row)._asdict()
        if target is None and not self._is_update_id(): del d['id']
        return d.items()
        return (target or self.row)._asdict().items()
    def _is_update_id(self): return hasattr(self.row, 'id') and hasattr(self.where, 'id') and self.row.id != self.where.id
    def _set_sql(self): return ', '.join(self._get_preperds())
    def _where_sql(self):
        where_sql = 'where id=?'
        if self.where is None: #更新するレコードを一意に特定する必要があります。引数whereがNoneのときはself.rowにid列と値を指定してください。id列がないテーブルのときは一意に特定できる列と値をget_self.row()で得た型のインスタンスで与えてください。
            if 'id' not in self.row._fields: raise ValueError('引数whereがNoneのときはself.rowにid列を指定してください。')
            if NOT_USE == self.row.id: raise ValueError('引数whereがNoneのときはself.rowにid列とその値を指定してください。')
        else:
            if isinstance(self.where, type): raise ValueError('引数whereは型でなくインスタンスを指定してください。')
            else: where_sql = 'where ' + ' and '.join(self._get_preperds(self.where))
        return where_sql
```

```python
class NtLite:
    def _update_sql_vals(self, row, where=None): return UpdateSqlBuilder(row, where).build()
    def update(self, row, where=None): return self._cast_exec(*self._update_sql_vals(row, where))
    def delete(self, row): return self._cast_exec(*DeleteSqlBuilder(row).build())
```

### 呼出例

```python
db = NtLite()
table_name = 'users'
db.exec(f"create table {table_name} (id integer, name text);")
db.inserts(table_name, [(0,'A'),(1,'B'),(2,'C')])
Row = db.get_row(table_name)
db.update(Row(id=0, name='a'))
db.delete(Row(id=1))
res = db.gets("select id num from users order by id asc;")
```

# 今後

　こうなると`insert`や`select`文も`Row`で呼び出せるようにしたい。

　`select`はたぶん複雑になるのでSQL文をほぼ直接書くことになる。簡単な`select`文はラップできるかもしれない。けど全件取得は件数が多すぎて実行しないと思うし。件数取得くらいかな？

　`where`句がついているだけならラップできそう。でも他に`order by`や`group by`、副問合せ用の`with`句、テーブル結合の`join`、SQL関数など色々ある。どこまでやるか。

　`create`文の作成は難しいかもしれない。むしろ`create`文から型や制約などの情報をもらうことになりそう。

