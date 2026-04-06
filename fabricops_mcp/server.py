from fastmcp import FastMCP

from fabricops_mcp.tools.stage1_inventory import register_stage1_tools
from fabricops_mcp.tools.stage2_pipeline import register_stage2_tools
from fabricops_mcp.tools.stage3_ops import register_stage3_ops_tools


mcp = FastMCP("FabricOps Agent")

register_stage1_tools(mcp)
register_stage2_tools(mcp)
register_stage3_ops_tools(mcp)


if __name__ == "__main__":
	mcp.run()
