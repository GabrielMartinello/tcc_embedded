import json
import os
import matplotlib.pyplot as plt


with open("benchmark_cargas.json", "r") as f:
    dados = json.load(f)

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

            for xi, yi in zip([p + deslocamento for p in x], valores_banco):
                ax.text(
                    xi,
                    yi + (yi * 0.02), 
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

        pasta = os.path.dirname(salvar_em)
        if not os.path.exists(pasta):
            os.makedirs(pasta)

        plt.savefig(salvar_em, dpi=300)
        print(f'Gráfico salvo em {salvar_em}')
        plt.close()

class GraficoBarrasPorConsulta:
    def __init__(self, dados_json):
        self.dados = dados_json["bancos"]

    def plot(self, salvar_em="graphs/consultas/"):
        # Descobrir todas as descrições únicas de consultas
        descricoes = set()
        for banco in self.dados:
            for consulta in banco["consultas"]:
                descricoes.add(consulta["descricao"])

        for descricao in descricoes:
            self._plot_consulta(descricao, salvar_em)

    def _plot_consulta(self, descricao_consulta, salvar_em):
        metricas = ["tempo_execucao_s", "memoria_max_utilizada", "cpu_percent"]
        labels_metricas = {
            "tempo_execucao_s": "Tempo (s)",
            "memoria_max_utilizada": "Memória (MB)",
            "cpu_percent": "CPU (%)"
        }

        bancos = [banco["nome"] for banco in self.dados]
        valores = {m: [] for m in metricas}

        for banco in self.dados:
            consulta = next(
                (c for c in banco["consultas"] if c["descricao"] == descricao_consulta), None
            )
            if consulta:
                for m in metricas:
                    valores[m].append(consulta.get(m, 0))
            else:
                for m in metricas:
                    valores[m].append(0)  # Se não tiver essa consulta no banco

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

            for xi, yi in zip([p + deslocamento for p in x], valores_banco):
                ax.text(
                    xi,
                    yi + (yi * 0.02) if yi != 0 else 0.1,
                    f'{yi:.2f}',
                    ha='center',
                    fontsize=9
                )

        ax.set_xticks([p + (largura_barra * (len(bancos) - 1) / 2) for p in x])
        ax.set_xticklabels([labels_metricas[m] for m in metricas])
        ax.set_ylabel("Valor")
        ax.set_title(f"Benchmark da Consulta: {descricao_consulta}")
        ax.legend()
        plt.tight_layout()

        pasta = os.path.join(salvar_em)
        if not os.path.exists(pasta):
            os.makedirs(pasta)

        nome_arquivo = descricao_consulta.replace(" ", "_").replace("/", "_")
        caminho = os.path.join(pasta, f"{nome_arquivo}.png")
        plt.savefig(caminho, dpi=300)
        print(f'Gráfico salvo em {caminho}')
        plt.close()

if __name__ == "__main__":
    grafico = GraficoBarrasAgrupadas(dados)
    grafico.plot()

    with open("resultados_benchmark.json", "r") as f:
        dados = json.load(f)

    grafico = GraficoBarrasPorConsulta(dados)
    grafico.plot()        
