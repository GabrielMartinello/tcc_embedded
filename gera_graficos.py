import json
import os
import matplotlib.pyplot as plt


# 🔸 Dados simulados (substitua por leitura de arquivo se quiser)
with open("benchmark_cargas.json", "r") as f:
    dados = json.load(f)


# 🔥 Classe para gerar o gráfico
class GraficoBarrasAgrupadas:
    def __init__(self, dados_json):
        self.dados = dados_json["cargas"]

    def plot(self, salvar_em='graphs/benchmark_carga.png'):
        metricas = ["tempo_execucao_segundos", "memoria_variacao_mb", "cpu_percent_medio"]
        labels_metricas = {
            "tempo_execucao_segundos": "Tempo (s)",
            "memoria_variacao_mb": "Memória (MB)",
            "cpu_percent_medio": "CPU (%)"
        }

        bancos = [d["banco"] for d in self.dados]
        valores = {m: [d[m] for d in self.dados] for m in metricas}

        x = range(len(metricas))
        largura_barra = 0.35

        fig, ax = plt.subplots(figsize=(10, 6))

        for i, banco in enumerate(bancos):
            deslocamento = i * largura_barra
            valores_banco = [valores[m][i] for m in metricas]
            ax.bar(
                [p + deslocamento for p in x],
                valores_banco,
                width=largura_barra,
                label=banco
            )

            # Adiciona os valores acima das barras
            for xi, yi in zip([p + deslocamento for p in x], valores_banco):
                ax.text(
                    xi,
                    yi + (yi * 0.02),  # pequeno deslocamento acima da barra
                    f'{yi:.1f}',
                    ha='center',
                    fontsize=9
                )

        ax.set_xticks([p + largura_barra / 2 for p in x])
        ax.set_xticklabels([labels_metricas[m] for m in metricas])
        ax.set_ylabel("Valor")
        ax.set_title("Benchmark de Carga entre SGBDs")
        ax.legend()
        plt.tight_layout()

        # 🔸 Cria a pasta se não existir
        pasta = os.path.dirname(salvar_em)
        if not os.path.exists(pasta):
            os.makedirs(pasta)

        # 🔥 Salva o gráfico como imagem PNG
        plt.savefig(salvar_em, dpi=300)
        print(f'✅ Gráfico salvo em {salvar_em}')
        plt.close()


# 🔥 Executar o gráfico
if __name__ == "__main__":
    grafico = GraficoBarrasAgrupadas(dados)
    grafico.plot()
