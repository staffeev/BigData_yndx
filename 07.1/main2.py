import pandas as pd
from matplotlib import pyplot as plt

df = pd.read_csv("eplleaguetables.csv")

fig, axs = plt.subplots(nrows=5, ncols=4, figsize=(25,20))
fig.suptitle("Разбросы очков команд в разбивке по местам", size=20)
flierprops = dict(marker='o', markerfacecolor='r', markersize=12,
                    linestyle='none', markeredgecolor='black')
c = 1
for i in range(5):
    for j in range(4):
        data = df[df["Position"] == c]
        bp = axs[i, j].boxplot(data["Points"], showfliers=True,
                                flierprops=flierprops)
        flag = False
        for flier in bp['fliers']:
            if flier.get_data()[0].size != 0: # выброс есть
                flag = True
                axs[i, j].set_title(f"{c} место, выбросы есть", size=7)
        alpha = 0.3 if not flag else 1.0
        title = f"{c} место, выбросы есть" if flag else f"{c} место"
        
        # наведение красоты
        axs[i, j].set_title(title, size=7, alpha=alpha)
        for element in ['boxes', 'whiskers', 'medians', 'caps']:
            for item in bp[element]:
                item.set_alpha(alpha)
        axs[i, j].spines['top'].set_alpha(alpha)
        axs[i, j].spines['right'].set_alpha(alpha)
        axs[i, j].spines['left'].set_alpha(alpha)
        axs[i, j].spines['bottom'].set_alpha(alpha)
        axs[i, j].get_xaxis().set_ticks([])
        for label in axs[i, j].get_xticklabels() + axs[i, j].get_yticklabels():
            label.set_alpha(alpha)
        c += 1
plt.show()