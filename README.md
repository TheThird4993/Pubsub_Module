# LKM: Publish/Subscribe Message Broker

## Sobre o Projeto
Este projeto consiste na implementação de um **Módulo Carregável de Kernel (LKM)** para o sistema operacional Linux. O módulo atua como um *broker* de mensagens entre processos em espaço de usuário, utilizando o padrão arquitetural **Publish/Subscribe**. A comunicação é realizada através do dispositivo de caractere `/dev/pubsub`.

O objetivo principal é gerenciar múltiplos tópicos e distribuir mensagens de forma assíncrona para os processos inscritos, garantindo o gerenciamento correto de memória no kernel e a prevenção de condições de corrida através de exclusão mútua (Mutex).

## Funcionamento e API

A interação dos processos de usuário com o driver é feita por meio de operações de escrita (`write`) e leitura (`read`) no arquivo `/dev/pubsub`, utilizando comandos em formato de texto. O rastreamento dos processos é feito através de seus respectivos PIDs.

### Comandos Suportados

| Comando | Operação | Descrição |
| :--- | :--- | :--- |
| `/subscribe <tópico>` | `write` | Inscreve o processo no tópico especificado. Se o tópico não existir, ele é criado. |
| `/unsubscribe <tópico>`| `write` | Remove a inscrição do processo, limpando suas mensagens pendentes. Se o tópico ficar sem inscritos, é destruído. |
| `/publish <tópico> <msg>`| `write` | Enfileira a mensagem fornecida para todos os processos atualmente inscritos no tópico. |
| `/fetch <tópico>` | `write` | Configura o contexto do processo para indicar de qual tópico a próxima leitura irá extrair dados. |
| **(Leitura)** | `read` | Consome uma única mensagem por vez da fila do tópico previamente selecionado via `/fetch`. Retorna `0` se a fila estiver vazia. |
| **(Fechamento)** | `release` | Ao fechar o arquivo (`fclose()`), o driver automaticamente desinscreve o processo de todos os tópicos e libera os recursos. |

## Configuração e Monitoramento

O módulo interage com diferentes subsistemas do Linux para configuração e exposição de dados:

* **Parâmetros do Módulo (`module_param`):** O número máximo de tópicos globais suportados pelo *broker* é definido no momento do carregamento do módulo.
* **Procfs (`/proc/pubsub`):** Expõe estatísticas em tempo real, exibindo a contagem total de mensagens publicadas em cada tópico ativo desde a sua criação (ex: `teste: 5`).

## Detalhes Técnicos e Sincronização

Para garantir a estabilidade do sistema operacional, o módulo implementa:

* **Estruturas de Dados:** Uso intensivo da API nativa de listas encadeadas circulares do Linux (`<linux/list.h>` / `struct list_head`). Essa interface padrão do kernel foi utilizada para gerenciar dinamicamente a lista global de tópicos, as listas de processos inscritos e as filas de mensagens pendentes de cada processo.
* **Gerenciamento de Memória:** Uso estrito de alocação dinâmica no kernel (`kmalloc` e `kfree`) e da API nativa de listas encadeadas do Linux, garantindo a ausência de vazamentos de memória (*memory leaks*).
* **Controle de Concorrência:** Implementação de `mutex` para prevenir condições de corrida (Race Conditions) e *deadlocks*. As travas protegem:
    * A lista global de tópicos (contra criações e deleções simultâneas via *subscribe/unsubscribe* e leitura simultânea no *procfs*).
    * A lista de processos inscritos em cada tópico (garantindo que um *publish* não itere em uma lista sendo modificada).
    * A fila de mensagens individual de cada processo (protegendo operações simultâneas de leitura e inserção).
