class PipelineCancelledException(Exception):
    """
    Excepción personalizada para señalizar que el usuario 
    ha solicitado detener el pipeline.
    """
    pass