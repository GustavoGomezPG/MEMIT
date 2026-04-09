import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { useState, useEffect } from "react";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "../../components/ui/card";
import { Input } from "../../components/ui/input";
import { Label } from "../../components/ui/label";
import { Button } from "../../components/ui/button";
import { Separator } from "../../components/ui/separator";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "../../components/ui/select";
import { PermissionsGuide } from "../../components/PermissionsGuide";
import {
  getServiceKeys,
  validateAndCreateKey,
  createMigration,
} from "../../server/migrations";
import {
  Eye,
  EyeOff,
  CheckCircle,
  XCircle,
  Loader2,
  Plus,
  Key,
} from "lucide-react";
import type { ServiceKey } from "../../db/schema";

export const Route = createFileRoute("/migrations/new")({
  loader: () => getServiceKeys(),
  component: NewMigration,
});

function NewMigration() {
  const initialKeys = Route.useLoaderData();
  const navigate = useNavigate();
  const [keys, setKeys] = useState<ServiceKey[]>(initialKeys);

  const [name, setName] = useState("");
  const [sourceKeyId, setSourceKeyId] = useState("");
  const [targetKeyId, setTargetKeyId] = useState("");
  const [saving, setSaving] = useState(false);

  // New key form
  const [newKeyName, setNewKeyName] = useState("");
  const [newKeyToken, setNewKeyToken] = useState("");
  const [showToken, setShowToken] = useState(false);
  const [addingKey, setAddingKey] = useState(false);
  const [keyError, setKeyError] = useState<string | null>(null);

  useEffect(() => {
    setKeys(initialKeys);
  }, [initialKeys]);

  async function handleAddKey() {
    if (!newKeyName.trim() || !newKeyToken.trim()) return;
    setAddingKey(true);
    setKeyError(null);
    try {
      const result = await validateAndCreateKey({
        data: { name: newKeyName.trim(), accessToken: newKeyToken.trim() },
      });
      if (!result.success) {
        setKeyError(result.error || "Validation failed");
        return;
      }
      if (result.key) {
        setKeys((prev) => [...prev, result.key!]);
        setNewKeyName("");
        setNewKeyToken("");
        setShowToken(false);
      }
    } catch {
      setKeyError("Failed to add key");
    } finally {
      setAddingKey(false);
    }
  }

  async function handleSave() {
    if (!sourceKeyId || !targetKeyId || !name.trim()) return;
    setSaving(true);
    try {
      const migration = await createMigration({
        data: {
          name: name.trim(),
          sourceKeyId: Number(sourceKeyId),
          targetKeyId: Number(targetKeyId),
        },
      });
      if (migration) {
        navigate({
          to: "/migrations/$id",
          params: { id: String(migration.id) },
        });
      }
    } finally {
      setSaving(false);
    }
  }

  const canSave =
    name.trim() &&
    sourceKeyId &&
    targetKeyId &&
    sourceKeyId !== targetKeyId;

  return (
    <div className="mx-auto max-w-2xl space-y-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">New Migration</h1>
        <p className="text-sm text-muted-foreground">
          Set up a new content migration between two HubSpot portals
        </p>
      </div>

      {/* Service Keys */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Key className="h-4 w-4" />
            Service Keys
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {keys.length > 0 && (
            <div className="space-y-2">
              {keys.map((key) => (
                <div
                  key={key.id}
                  className="flex items-center justify-between rounded-md border bg-muted/30 px-3 py-2"
                >
                  <div>
                    <p className="text-sm font-medium">{key.name}</p>
                    <p className="text-xs text-muted-foreground font-mono">
                      Portal {key.portalId || "—"}
                    </p>
                  </div>
                  <CheckCircle className="h-3.5 w-3.5 text-green-600" />
                </div>
              ))}
            </div>
          )}

          <Separator />

          <div className="space-y-3">
            <p className="text-sm font-medium">Add a new key</p>
            <div className="grid gap-3 sm:grid-cols-2">
              <div className="space-y-1">
                <Label htmlFor="key-name" className="text-xs">
                  Key Name
                </Label>
                <Input
                  id="key-name"
                  placeholder='e.g. "Datamax Arkansas"'
                  value={newKeyName}
                  onChange={(e) => setNewKeyName(e.target.value)}
                />
              </div>
              <div className="space-y-1">
                <Label htmlFor="key-token" className="text-xs">
                  Access Token
                </Label>
                <div className="relative">
                  <Input
                    id="key-token"
                    type={showToken ? "text" : "password"}
                    placeholder="pat-na1-..."
                    value={newKeyToken}
                    onChange={(e) => {
                      setNewKeyToken(e.target.value);
                      setKeyError(null);
                    }}
                    className="pr-10"
                  />
                  <button
                    type="button"
                    className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                    onClick={() => setShowToken(!showToken)}
                  >
                    {showToken ? (
                      <EyeOff className="h-4 w-4" />
                    ) : (
                      <Eye className="h-4 w-4" />
                    )}
                  </button>
                </div>
              </div>
            </div>
            {keyError && (
              <div className="flex items-center gap-1.5 text-sm">
                <XCircle className="h-3.5 w-3.5 text-destructive" />
                <span className="text-destructive">{keyError}</span>
              </div>
            )}
            <Button
              variant="outline"
              size="sm"
              onClick={handleAddKey}
              disabled={!newKeyName.trim() || !newKeyToken.trim() || addingKey}
            >
              {addingKey ? (
                <Loader2 className="mr-1.5 h-3.5 w-3.5 animate-spin" />
              ) : (
                <Plus className="mr-1.5 h-3.5 w-3.5" />
              )}
              Validate & Add Key
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Migration Setup */}
      <Card>
        <CardHeader>
          <CardTitle>Migration Details</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="name">Project Name</Label>
            <Input
              id="name"
              placeholder='e.g. "Datamax Arkansas 2026"'
              value={name}
              onChange={(e) => setName(e.target.value)}
            />
          </div>

          <div className="grid gap-4 sm:grid-cols-2">
            <div className="space-y-2">
              <Label>Source Portal</Label>
              <Select value={sourceKeyId} onValueChange={(v) => setSourceKeyId(v ?? "")}>
                <SelectTrigger className="w-full">
                  <SelectValue placeholder="Select source..." />
                </SelectTrigger>
                <SelectContent>
                  {keys.map((key) => (
                    <SelectItem key={key.id} value={String(key.id)}>
                      {key.name} ({key.portalId || "—"})
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-2">
              <Label>Target Portal</Label>
              <Select value={targetKeyId} onValueChange={(v) => setTargetKeyId(v ?? "")}>
                <SelectTrigger className="w-full">
                  <SelectValue placeholder="Select target..." />
                </SelectTrigger>
                <SelectContent>
                  {keys.map((key) => (
                    <SelectItem key={key.id} value={String(key.id)}>
                      {key.name} ({key.portalId || "—"})
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          {sourceKeyId && targetKeyId && sourceKeyId === targetKeyId && (
            <p className="text-sm text-destructive">
              Source and target must be different portals.
            </p>
          )}

          <div className="pt-2">
            <Button
              onClick={handleSave}
              disabled={!canSave || saving}
            >
              {saving && (
                <Loader2 className="mr-1.5 h-4 w-4 animate-spin" />
              )}
              Save Migration
            </Button>
          </div>
        </CardContent>
      </Card>

      <PermissionsGuide />
    </div>
  );
}
