defmodule Worker do
  @moduledoc """
  Simple worker module for testing purposes
  """

  @doc """
  Default perform function for job processing
  """
  def perform(args) do
    # Simple worker that just returns the processed arguments
    {:ok, args}
  end

  @doc """
  Process function for job processing with explicit documentation
  """
  def process(args) do
    # Simple worker that just returns the processed arguments
    {:ok, args}
  end
end
