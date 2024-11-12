import pandas as pd
from matplotlib import pyplot as plt

df = pd.read_csv("sessions.csv")
bins = [0, 420, 840, 1260, 1680, df['duration'].max()]
labels = ['<7', '7-14', '14-21', '21-28', '>28']
df['duration_bins'] = pd.cut(df['duration'], bins=bins, labels=labels, right=False)
duration_counts = df['duration_bins'].value_counts().sort_index()
bins_value = df[df["has_purchase"] == True].groupby("duration_bins")["value"].agg("sum")

fig, ax1 = plt.subplots(figsize=(10, 6))

duration_counts.plot(kind='bar', color='#9BE0AA', edgecolor='black', ax=ax1, alpha=0.7,
                     width=0.6)
ax1.set_title('Распределение количества сессий по корзинам длительности')
ax1.set_xlabel("Корзины длительности (мин)")
y1labels = [f'{int(x)} тыс' for x in ax1.get_yticks() / (10 ** 3)]
ax1.set_yticklabels(y1labels)
ax1.set_ylabel('Количество сессий')
ax1.set_xticklabels(labels, rotation=0)

ax2 = ax1.twinx()
bins_value.plot(kind="line", color="salmon", ax=ax2, linewidth=5)
y2labels = [f'{x: .2f} млрд' for x in ax2.get_yticks() / (10 ** 9)]
ax2.set_yticklabels(y2labels)
ax2.set_ylabel("Прибыль")
plt.show()


# Комментарий
# Мне показалось, что только распределения данных по корзинам длительности недостаточно,
# так как в принципе в отрыве от всего остального эта информацция бесполезна (мы имеем
# данные чего-то "торгового", соответственно, ничего удивительного, что большинство пользователей
# торчат на нашем сайте/сервисе до 14 минут (т.е. недолго, посмотрели - заказали (возможно) - ушли).
# Плюс в данных нет выбросов, так что количество сессий закономерно уменьшается с увеличением их длительности,
# график получается особо не полезный.
# Так что я добавил еще дополнительный график прибыли компании в разбивке по корзинам длительности, и
# теперь можно делать какие-то полезные выводы, например, большинство покупок совершается пользователями,
# проведшими на сайте/сервисе 7-14 минут. Меньше этого времени прибыль гораздо ниже (хотя количество сессий больше)