-- Overlay Roblox asset-id loading probe.
-- Usage:
-- getgenv().OverlayRobloxAssetId = 1234567890
-- loadstring(game:HttpGet(".../runtime/roblox_asset_id_test.lua"))()

local GLOBAL = typeof(getgenv) == "function" and getgenv() or _G
local ASSET_ID = tonumber(GLOBAL.OverlayRobloxAssetId)

local function log(...)
	print("[overlay-roblox-asset-test]", ...)
end

if not ASSET_ID then
	error("Set getgenv().OverlayRobloxAssetId to a Roblox model asset id first")
end

local function stripScripts(root)
	for _, descendant in ipairs(root:GetDescendants()) do
		if descendant:IsA("Script") or descendant:IsA("LocalScript") then
			descendant:Destroy()
		end
	end
end

local function prepPart(part)
	part.Anchored = false
	part.CanCollide = false
	part.CanTouch = false
	part.CanQuery = false
	part.Massless = true
end

local function prepModel(root)
	stripScripts(root)
	for _, descendant in ipairs(root:GetDescendants()) do
		if descendant:IsA("BasePart") then
			prepPart(descendant)
		end
	end
end

local function tryLoad(value)
	local ok, objects = pcall(function()
		return game:GetObjects(value)
	end)
	if not ok then
		return nil, tostring(objects)
	end
	if type(objects) ~= "table" or #objects == 0 then
		return nil, "game:GetObjects returned no objects"
	end
	return objects
end

local assetUrl = "rbxassetid://" .. tostring(ASSET_ID)
log("loading", assetUrl)

local objects, err = tryLoad(assetUrl)
if not objects then
	objects, err = tryLoad(tostring(ASSET_ID))
end
if not objects then
	error("Roblox asset load failed: " .. tostring(err))
end

local folder = workspace:FindFirstChild("OverlayRobloxAssetTest")
if folder then
	folder:Destroy()
end
folder = Instance.new("Folder")
folder.Name = "OverlayRobloxAssetTest"
folder.Parent = workspace

for index, object in ipairs(objects) do
	prepModel(object)
	object.Name = "RobloxAssetLoaded_" .. tostring(index) .. "_" .. object.Name
	object.Parent = folder
	if object:IsA("Model") then
		pcall(function()
			object:PivotTo(CFrame.new(0, 8, -8))
		end)
	elseif object:IsA("BasePart") then
		object.CFrame = CFrame.new(0, 8, -8)
	end
end

log("loaded Roblox asset objects:", #objects, "parented to workspace.OverlayRobloxAssetTest")
