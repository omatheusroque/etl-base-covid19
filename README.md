## ETL dados Covid-19 base do SEADE.

#### **Escopo**
Esse script tem como finalidade fazer o processo de extração dos dados da base do SEADE no github, transformá-lo de modo que fique conforme o necessário e fazer a ingestão dos dados em uma base própria com a finalidade de alimentar um bot do Telegram e um Dashboard.

#### **Ambiente e execução**
O script está rodando em um servidor Ubuntu na nuvem da Oracle, com execução agendada através do CRON.

#### **Notificação de conclusão via Telegram**
Após a execução, o script notifica a conclusão da tarefa através do Telegram do responsável.