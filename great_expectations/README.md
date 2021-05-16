# Создание учебного проекта Great expectation

## Настройка pyton environment

Устанавливаем стандартный пакет для работы с разными окружениями

```
sudo apt install python3-venv
```

Создаем новое окружение

```
python3 -m venv ~/App/projects/de-training/python-venv-GE
```

Активируем новое окружение

```
. ~/App/projects/de-training/python-venv-GE/bin/activate
(python-venv-GE) user@localhost:~/App/projects/de-training$
```

Устанавливаем в окружение пакеты для работы с Great expectations.

```
pip3 install psycopg2-binary
pip3 install wheel
pip3 install 'great-expectations==0.13.14'
pip3 install 'SQLAlchemy==1.3.24'
```

## Создание проекта Great expectation

Инициируем новый проект

```
cd ~/App/projects/de-training
git clone https://github.com/alexandersamoylov/de-training-GE.git
cd ~/App/projects/de-training/de-training-GE
great_expectations init
```

В интерактивном режиме вносим параметры подключения к базе данных.

Если при инициализации набор с demo проверками не добавлялся, можно добавить командой:

```
great_expectations suite demo
```

Если нужно добавить новый пустой набор проверок, добавляем командой:

```
great_expectations suite new
```

Для перехода к редактированию существующего набора проверок:

```
great_expectations suite edit <suite name>
```

После создания наборов проверок для всех источников, получаем список:

```
great_expectations suite list
Using v2 (Batch Kwargs) API
4 Expectation Suites found:
 - asamoilov.ods_billing.warning
 - asamoilov.ods_issue.warning
 - asamoilov.ods_payment.warning
 - asamoilov.ods_traffic.warning
```

Для каждого набора проверок, создаем checkpoint, в интерактивном режиме указываем параметры подключения к таблице с  данными, пример вызова команды:

```
great_expectations checkpoint new ods_payment asamoilov.ods_payment.warning
```

Итоговый список checkpoint:

```
great_expectations checkpoint list
Using v2 (Batch Kwargs) API
Heads up! This feature is Experimental. It may change. Please give us your feedback!
Found 4 checkpoints.
 - ods_billing
 - ods_traffic
 - ods_payment
 - ods_issue
```

Для получения свежего отчета по данным, запускаем выполнение:

```
great_expectations checkpoint run ods_billing
great_expectations checkpoint run ods_traffic
great_expectations checkpoint run ods_payment
great_expectations checkpoint run ods_issue
```

> Команда run возвращает код ошибки, если проверки завершаются неуспешно. Подходит для использования в пайплайне загрузки перед загрузкой в детальный слой.

Пример готовых отчетов:

[data_docs.zip](data_docs.zip)

## Список проверок для таблиц ODS

### ods_payment

#### Table Expectation(s)

```
# Проверка на кол-во строк
batch.expect_table_row_count_to_be_between(min_value=9000, max_value=11000)

# Проверка на кол-во атрибутов
batch.expect_table_column_count_to_equal(value=9)

# Проверка порядка атрибутов
batch.expect_table_columns_to_match_ordered_list(column_list=['user_id', 'pay_doc_type', 'pay_doc_num', 'account', 'phone', 'billing_period', 'pay_date', 'payment_sum', 'date_part_year'])

# Проверка уникальности значений для pay_doc_type, pay_doc_num
# Условие взято из описания таблицы в ДЗ-1, на текущем тестовом наборе данных - failed.
batch.expect_compound_columns_to_be_unique(column_list=['pay_doc_type', 'pay_doc_num'])
```

#### Column Expectation(s)

```
# Проверки обязательных атрибутов на значения null
batch.expect_column_values_to_not_be_null(column='user_id')
batch.expect_column_values_to_not_be_null(column='pay_doc_type')
batch.expect_column_values_to_not_be_null(column='pay_doc_num')
batch.expect_column_values_to_not_be_null(column='account')
batch.expect_column_values_to_not_be_null(column='phone')
batch.expect_column_values_to_not_be_null(column='billing_period')
batch.expect_column_values_to_not_be_null(column='pay_date')
batch.expect_column_values_to_not_be_null(column='payment_sum')

# Значение платежной системы(pay_doc_type) должно входить в множество: 
# MASTER, MIR, VISA
batch.expect_column_distinct_values_to_be_in_set(column='pay_doc_type', value_set=['MASTER', 'MIR', 'VISA'])

# В каждой загрзуке должны присутствовать записи по всем трем платежным
# системам(pay_doc_type)
batch.expect_column_distinct_values_to_equal_set(column='pay_doc_type', value_set=['MASTER', 'MIR', 'VISA'])

# Проверка строки с номером телефона на соответствие формату: +79991112233
batch.expect_column_values_to_match_regex(column='phone', regex='^\+\d{11}')

# Проверка суммы платежей. Платежи должны быть положительным числом, 
# не должны быть аномально большими. Берем для верхней границы +1 порядок 
# от максимального платежа.
batch.expect_column_values_to_be_between(column='payment_sum', min_value=0.00, max_value=19999)
```

### ods_traffic

#### Table Expectation(s)

```
# Проверка на кол-во строк
batch.expect_table_row_count_to_be_between(min_value=9000, max_value=11000)

# Проверка на кол-во атрибутов
batch.expect_table_column_count_to_equal(value=7)

# Проверка порядка атрибутов
batch.expect_table_columns_to_match_ordered_list(column_list=['user_id', 'traffic_time', 'device_id', 'device_ip_addr', 'bytes_sent', 'bytes_received', 'date_part_year'])
```

#### Column Expectation(s)

```
# Проверки обязательных атрибутов на значения null
batch.expect_column_values_to_not_be_null(column='user_id')
batch.expect_column_values_to_not_be_null(column='traffic_time')
batch.expect_column_values_to_not_be_null(column='device_id')
batch.expect_column_values_to_not_be_null(column='device_ip_addr')
batch.expect_column_values_to_not_be_null(column='bytes_sent')
batch.expect_column_values_to_not_be_null(column='bytes_received')

# Проверка ip адреса на соответствие шаблону
batch.expect_column_values_to_match_regex(column='device_ip_addr', regex='\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}')

# Проверка на положительные значения для bytes_sent, bytes_received
batch.expect_column_values_to_be_between(column='bytes_sent', min_value=0)
batch.expect_column_values_to_be_between(column='bytes_received', min_value=0)
```

### ods_billing

#### Table Expectation(s)

```
# Проверка на кол-во строк
batch.expect_table_row_count_to_be_between(min_value=9000, max_value=11000)

# Проверка на кол-во атрибутов
batch.expect_table_column_count_to_equal(value=7)

# Проверка порядка атрибутов
batch.expect_table_columns_to_match_ordered_list(column_list=['user_id', 'billing_period', 'service', 'tariff', 'billing_sum', 'created_at', 'date_part_year'])
```

#### Column Expectation(s)

```
# Проверки обязательных атрибутов на значения null
batch.expect_column_values_to_not_be_null(column='user_id')
batch.expect_column_values_to_not_be_null(column='billing_period')
batch.expect_column_values_to_not_be_null(column='service')
batch.expect_column_values_to_not_be_null(column='tariff')
batch.expect_column_values_to_not_be_null(column='billing_sum')
batch.expect_column_values_to_not_be_null(column='created_at')

# Проверка списка сервисов на попадание в множество:
batch.expect_column_distinct_values_to_be_in_set(column='service', value_set=['Домашний интернет', 'Цифровое ТВ'])

# Проверка тарифного плана на попадание в множество:
# Maxi, Mini, Gigabyte, Megabyte
batch.expect_column_distinct_values_to_be_in_set(column='tariff', value_set=['Базовый 200', 'Базовый 400', 'Выгодный 300', 'Выгодный 500'])

# Проверка суммы счетов. Суммы должны быть положительным числом,
# не должны быть аномально большими.
batch.expect_column_values_to_be_between(column='billing_sum', max_value=19999, min_value=0.00)
```

### ods_issue

#### Table Expectation(s)

```
# Проверка на кол-во строк
batch.expect_table_row_count_to_be_between(min_value=9000, max_value=11000)

# Проверка на кол-во атрибутов
batch.expect_table_column_count_to_equal(value=7)

# Проверка порядка атрибутов
batch.expect_table_columns_to_match_ordered_list(column_list=['user_id', 'start_time', 'end_time', 'title', 'description', 'service', 'date_part_year'])
```

#### Column Expectation(s)

```
# Проверки обязательных атрибутов на значения null
batch.expect_column_values_to_not_be_null(column='user_id')
batch.expect_column_values_to_not_be_null(column='start_time')
batch.expect_column_values_to_not_be_null(column='title')
batch.expect_column_values_to_not_be_null(column='description')
batch.expect_column_values_to_not_be_null(column='service')

# Проверка попадания значений для service в множество возможных вариантов:
# Connect, Disconnect, Setup Environment
batch.expect_column_distinct_values_to_be_in_set(column='service', value_set=['Домашний интернет', 'Цифровое ТВ'])
```

