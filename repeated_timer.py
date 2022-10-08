import threading
import time

class RepeatedTimer:
  def __init__(self, interval, function, *args, **kwargs):
    self._timer = None
    self.interval = interval
    self.function = function
    self.args = args
    self.kwargs = kwargs
    self.is_running = False
    self.next_call = time.time()
    self.start() # Se ejecuta en la creación del objeto, es el punto de entrada del objeto por así decir

  def _run(self):
    # Tras pasar el intervalo definido, se ejecuta el objeto Timer y esta funcion entra en ejecucion. Primero define
    # a False el is_running para que asi cuando vaya a la función start quede programada la siguiente iteracion.
    # Si no me equivoco, casi toodo el delay que hay se debe a lo que se tarda en cambiar el atributo y en ejecutar esta
    # la funcion start()
    self.is_running = False
    # Como se describe antes, se ejecuta este método para ir preparando ya la siguiente iteración
    self.start()
    # Por fin, la funcion que nos interesa que se repita es ejecutada.
    self.function(*self.args, **self.kwargs)

  def start(self):
    """
    Esta funcion lo que hace es ejecutarse una y otra vez
    :return:
    """
    if not self.is_running:
      # Calula cuando tiene que ocurrir la siguiente iteracion
      self.next_call += self.interval
      # Crea la tarea de ejecutar el self._run para dentro del tiempo especificado en interval
      # (a través de self.next_call)
      try:
        self._timer = threading.Timer(self.next_call - time.time(), self._run)
        # No puedo usar esto del daemon ya que si python detecta que no hay nada corriendo que no sea un daemon se para
        #self._timer.daemon = True
        # La iteracion que ha sido programada debe ser iniciada, por lo que se usa su método .start()
        self._timer.start()
      except BaseException as e:
          print("Estoy en la exception")
          print(e)
          print(e.__class__)
          print(self.args)
      # Define que se esta corriendo codigo tras activar la siguiente iteracion (que será dentro de self.interval
      # segundos) con el método start
      self.is_running = True

  def stop(self):
    self._timer.cancel()
    self.is_running = False



